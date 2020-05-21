from selenium import webdriver
from selenium.common.exceptions import WebDriverException, NoSuchWindowException, TimeoutException, \
    NoAlertPresentException, ElementNotInteractableException, NoSuchElementException, UnexpectedAlertPresentException, \
    ElementNotVisibleException
from threading import Thread
import pandas as pd
from queue import Queue
from collections import namedtuple
from util import Logger, purify, is_valid_web
from argparse import ArgumentParser
from win32api import GetSystemMetrics
import requests
from typing import List, Tuple, Dict
import time
import re
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib3.util import parse_url
from os.path import join
import difflib
import traceback
import sys
import json

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
WebInfo = namedtuple('WebInfo', ['url', 'form_data'])


# TODO 自动填写默认账号密码
class Scheduler:
    """调度器
    调度器负责：
    1. 启动每个子进程
        1. Worker
        2. Daemon
    2. 分发任务
    """

    def __init__(self, file_path, file_sep='\t', file_encoding='utf8', file_type='csv'):
        self.file_path = file_path
        self.file_sep = file_sep
        self.file_encoding = file_encoding
        self.file_type = file_type
        self.all_queue = Queue()
        self._spawn_data()
        self.workers = []
        self.validators = []

    def _spawn_data(self) -> None:
        self.__spawn_data_from_file()

    @staticmethod
    def _add_recode(df: pd.DataFrame) -> pd.DataFrame:
        def gen_default_kvs(arr: list) -> str:
            da = dict([(ele, "admin") for ele in arr[:-1]])
            db = {arr[-1]: "123456"}
            da.update(db)
            return json.dumps(da)

        df_tmp = df.groupby("REFERER").apply(lambda _df: _df.head(1))
        df_tmp.reset_index(inplace=True, drop=True)
        df_tmp["kvs"] = df_tmp["names"].apply(lambda row: gen_default_kvs(json.loads(row)[-1]))
        return pd.concat([df, df_tmp], axis=0)

    def __spawn_data_from_file(self) -> None:
        """
        从文件中生成数据
        支持两种格式：
            1. csv
            2. xlsx
        :return:
        """
        if self.file_type == 'csv':
            with open(self.file_path, encoding=self.file_encoding, errors='ignore') as f:
                df = pd.read_csv(f, sep=self.file_sep, encoding=self.file_encoding)
        elif self.file_type == 'xlsx':
            df = pd.read_excel(self.file_path, encoding=self.file_encoding)
        else:
            raise Exception("Can not load file from type: {}".format(self.file_type))
        # df = self._add_recode(df)
        self.total_num = len(df)
        # DONE 不再放入账号密码字段，直接放预处理之后的form_data
        for line in df.itertuples():
            url = line[4]
            form_data = line[5]
            form_data = purify(form_data)
            self.all_queue.put((url, form_data))

    @staticmethod
    def __window_schema(window_num) -> List[Tuple[int, int, int, int]]:
        """
        窗口划分逻辑
        :param window_num:
        :return:
        """
        W, H = GetSystemMetrics(0), GetSystemMetrics(1)
        if window_num == 4:
            w = W // 2
            h = H // 2
        return [(0, 0, w, h), (w, 0, w, h), (0, h, w, h), (w, h, w, h)]

    def scheduling(self):
        workers_info = []
        LOG.debug("[Scheduler]SchedulerLength of queue: {}".format(self.all_queue.qsize()))

        layout_list = self.__window_schema(THREADS_NUM)

        LOG.debug("[Scheduler]Spawning the Workers.")
        for i in range(THREADS_NUM):
            worker = Worker(self.all_queue, layout_list[i], i + 1)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
            workers_info.append((worker, layout_list[i], i + 1))

        d = Daemon(workers_info, self.all_queue, self.total_num)
        d.daemon = True
        d.start()
        self.all_queue.join()


class Worker(Thread):
    def __init__(self, queue, layout_params, tid):
        Thread.__init__(self)
        self.queue = queue
        self.layout_params = layout_params
        self.tid = tid
        self.status = 'Launching...'
        self._exit_code = 0
        self._exit_info = ''
        self._exception = ''
        self._exc_traceback = ''

    def __init(self) -> None:
        """
        设置浏览器属性并打开浏览器
        设置driver属性
        :return:
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-gpu')
        # 不加载图片
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # chrome_options.add_experimental_option("prefs", prefs)
        # chrome_options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(chrome_options=chrome_options)
        self.status = 'Started.'
        self.driver.set_window_position(self.layout_params[0], self.layout_params[1])
        self.driver.set_window_size(self.layout_params[2], self.layout_params[3])
        self.driver.implicitly_wait(0.5)
        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(60)

    def __fill(self, key, value) -> None:
        """
        以给定的name来查找元素，以给定的value来填表
        :param key:
        :param value:
        :return:
        """
        type_dict = {
            "name": self.driver.find_element_by_name,
            "id": self.driver.find_element_by_id
        }

        def fill_by(type, key, value):
            try:
                ele = type_dict[type](key)
                ele.clear()
                ele.send_keys(value)
            except (ElementNotInteractableException, ElementNotVisibleException):
                js = "var item = document.getElementById('{}');item.type=''".format(key)
                self.driver.execute_script(js)
                ele = type_dict[type](key)
                ele.clear()
                ele.send_keys(value)
            except UnexpectedAlertPresentException:
                self.driver.switch_to.alert.accept()
                ele = type_dict[type](key)
                ele.clear()
                ele.send_keys(value)
            except NoSuchElementException:
                LOG.debug("[Worker]Worker: {} No such element type: {}, key: {}".format(self.tid, type, key))

        for _type in type_dict.keys():
            fill_by(_type, key, value)

    def __submit(self) -> None:
        """
        寻找登录按钮并点击
        :return:
        """
        xpaths = ("//button[@type='submit']", "//input[@type='submit']")
        for xpath in xpaths:
            try:
                ele = self.driver.find_element_by_xpath(xpath)
                ele.click()
            except NoSuchElementException:
                continue

    def __execute_js(self, js: str) -> None:
        try:
            self.driver.execute_script(js)
        except TimeoutException:
            self.driver.close()

    def _fill_ele(self, name: str, value: str, tag: str = '') -> None:
        """
        根据全文信息填表
        DONE 如果没有填上可不可以把form_data打出来，不要再去看了
        :param name:
        :param value:
        :param tag: {Account， Password， Safecode， Others}
        :return:
        """
        LOG.debug("[Worker]Worker: {} Finding the {} location.".format(self.tid, tag))
        self.status = 'Finding the account location.'

        t = time.time()
        try:
            self.__fill(name, value)
            LOG.debug("[Worker]Worker: {} {} location found.".format(self.tid, tag))
        except AttributeError:
            LOG.debug(
                "[Worker]Worker: {} Unable to find {}! "
                "Detail: No such element name!".format(self.tid, tag))
        except WebDriverException as e:
            LOG.debug(
                "[Worker]Worker: {} Unable to find {}! "
                "Detail: {}.".format(self.tid, tag, repr(e)))
        LOG.debug("[Worker]Worker: {} Finding the {} location cost time: {}s.".format(
            self.tid, tag, round(time.time() - t, 3)))

    def _click_submit(self) -> None:
        LOG.debug("[Worker]Worker: {} Finding the submit button.".format(self.tid))
        self.status = "Finding the submit button."
        t = time.time()
        if IS_AUTO_SUBMIT:
            try:
                self.__submit()
                LOG.debug("[Worker]Worker: {} Password location found.".format(self.tid))
            except AttributeError:
                LOG.debug("[Worker]Worker: {} Unable to find element!".format(self.tid))
            except UnexpectedAlertPresentException:
                self.driver.switch_to.alert.accept()
            except WebDriverException:
                LOG.debug("[Worker]Worker: {} Unknown errors.".format(self.tid))
            LOG.debug(
                "[Worker]Worker: {} Finding the submit button location cost time: {}s.".format(
                    self.tid, round(time.time() - t, 3)))
            # 登录失败之后可能有alert弹窗，因此需要点掉alert弹窗
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass

    def _gen_tab(self) -> Tuple[str, str]:
        try:
            main_win = self.driver.current_window_handle
            all_win = self.driver.window_handles
        except NoSuchWindowException:
            all_win = self.driver.window_handles
            main_win = all_win[0]
            self.driver.switch_to.window(main_win)
        sub_win = ""
        if len(all_win) == 1:
            url = join(RUN_PATH, "index.html").replace("\\", "/")
            self.driver.execute_script('window.open("about:blank")')
            all_win = self.driver.window_handles
            sub_win = all_win[1]
            self.driver.switch_to.window(sub_win)
            self.driver.get(url)
            # 还是定位在main_win上的
            self.driver.switch_to.window(main_win)
        return main_win, sub_win

    def _is_reachable(self, url) -> bool:
        retries = 2
        count = 0
        while count < retries:
            LOG.debug("[Worker]Worker: {} Requesting the url: {}, Retries: {}.".format(self.tid, url, count))
            self.status = 'Requesting, Retries: {}.'.format(count)
            try:
                self.driver.get(url)
                # 可能有Alert弹窗
                # TODO 其他弹窗
                try:
                    self.driver.switch_to.alert.accept()
                except NoAlertPresentException:
                    pass
                content = self.driver.page_source
                if is_valid_web(content, LOG):
                    return True
                else:
                    count += 1
            except TimeoutException:
                count += 1
        return False

    def _log_in_sub_title(self, msg, main_win, sub_win) -> None:
        """
        利用次Tab的Title来显示信息
        :param msg:
        :return:
        """
        self.driver.switch_to.window(sub_win)
        self.__execute_js("document.title = '{}...'".format(msg))
        self.driver.switch_to.window(main_win)

    def _log_in_input(self, msg, main_win, sub_win) -> None:
        self.driver.switch_to.window(sub_win)
        ele = self.driver.find_element_by_name("form_data")
        ele.send_keys(msg)
        self.driver.switch_to.window(main_win)

    def run(self) -> None:
        """
        最重要的问题就是`NoSuchWindowException`
        整个运行过程都要防止没有窗口的问题
        :return:
        """
        try:
            import time
            import sys
            import json
            self.__init()
            while True:
                LOG.debug("[Worker]Worker: {} Getting web info".format(self.tid))
                url, kvs = self.queue.get()
                LOG.debug("[Worker]Worker: {} Length of queue: {}".format(self.tid, self.queue.qsize()))
                LOG.debug("[Worker]Worker: {} webinfo: {}".format(self.tid, kvs))

                # 两个Tab，main_win是主窗口，用来验证网站, sub_win是副窗口，用来打印参数
                main_win, _sub_win = self._gen_tab()
                sub_win = ""
                if _sub_win:
                    sub_win = _sub_win

                LOG.debug("[Worker]Worker: {} Requesting the url.".format(self.tid))
                self.status = 'Requesting.'
                self._log_in_sub_title("网页打开中...", main_win, sub_win)
                if not self._is_reachable(url):
                    LOG.debug("[Worker]Worker: {} Request time out.".format(self.tid))
                    self.__execute_js("window.stop()")
                    continue
                # 填表
                # 当无法登陆时，可能会清楚已填写的账号密码安全码，因此不管是否登录，都再填一遍，如果登陆了就找不到元素，就填不上，没有影响
                self._log_in_sub_title("尝试登录中...", main_win, sub_win)

                self._log_in_input(json.dumps(kvs), main_win, sub_win)
                for k, v in kvs.items():
                    self._fill_ele(k, v)
                self._click_submit()
                time.sleep(0.5)
                for k, v in kvs.items():
                    self._fill_ele(k, v)

                self._log_in_sub_title("等待关闭...", main_win, sub_win)
                LOG.debug("[Worker]Worker: {} Waiting to be closed.".format(self.tid))
                self.status = "Waiting to be closed."
                while True:
                    all_win = self.driver.window_handles
                    if main_win not in all_win:
                        break
                    time.sleep(2)
                self.queue.task_done()
        except Exception as e:
            LOG.debug("[Worker]Worker: {} is dead, error info: {}".format(self.tid, repr(e)))
            self._exit_code = 1
            self._exception = e
            self._exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            LOG.debug("[Worker]Worker: {} Traceback: {}.".format(self.tid, self._exc_traceback))
            self.queue.put((url, kvs))
            self.driver.quit()


class Daemon(Thread):
    def __init__(self,
                 workers_info: List[Tuple[Worker, Tuple, int]],
                 all_queue,
                 total_num):
        Thread.__init__(self)
        self.workers_info = workers_info
        self.all_queue = all_queue
        self.total_num = total_num

    def run(self) -> None:
        import time
        while True:
            time.sleep(5)
            LOG.info("=" * 60)
            LOG.info("[Daemon]ALL_QUEUE: {}/{}".format(self.all_queue.qsize(), self.total_num))

            for w, _, tid in self.workers_info:
                LOG.info("[Daemon]Worker: {} Status: {}, {}".format(
                    tid,
                    'Alive' if w.is_alive() else 'Dead',
                    w.status))
            LOG.info("=" * 60)
            for w, layout_params, tid in self.workers_info:
                if not w.is_alive():
                    LOG.debug("[Daemon]Worker: {} is dead, spawn a new thread instead.".format(tid))
                    self.workers_info.remove((w, layout_params, tid))
                    w = Worker(self.all_queue, layout_params, tid)
                    w.daemon = True
                    w.start()
                    self.workers_info.append((w, layout_params, tid))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p", "--path", action="store", dest="path", default='./datas/filtered.csv', type=str,
                        help="Data path.")
    parser.add_argument("-t", "--file_type", action="store", dest="file_type", default='csv', type=str,
                        help="Data type.")
    parser.add_argument("-n", "--thread_num", action="store", dest="thread_num", default=4, type=int,
                        help="Number of validator threads.")
    parser.add_argument("-r", "--retries", action="store", dest="retries", default=5, type=int,
                        help="Number of retry times.")
    parser.add_argument("-tm", "--timeout", action="store", dest="timeout", default=30, type=int,
                        help="Timeout.")
    parser.add_argument("-u", "--IS_AUTO_SUBMIT", action="store_true", dest="IS_AUTO_SUBMIT", default=True,
                        help="Timeout.")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug", help="Debug mode.")
    parser.add_argument("-s", "--test", action="store_true", dest="test", help="Using test data.")

    args = parser.parse_args()
    path = args.path
    file_type = args.file_type
    THREADS_NUM = args.thread_num
    RETRIES = args.retries
    TIMEOUT = args.timeout
    IS_AUTO_SUBMIT = args.IS_AUTO_SUBMIT
    is_debug = args.debug
    is_test = args.test
    RUN_PATH = sys.path[0]

    LOG, RQ = Logger('CrawlerLog', is_debug).getlog()

    if is_test:
        data_path = './datas/web_test.csv'
    else:
        data_path = path
    sch = Scheduler(data_path, file_type=file_type)
    LOG.info("Start Scheduler.\n"
             "Number of Workers: {}\n"
             "Number of web sites: {}\n"
             "RETRIES: {}\n"
             "TIMEOUT: {}\n"
             "IS_AUTO_SUBMIT: {}\n"
             "is_debug: {}\n".format(THREADS_NUM, sch.total_num, RETRIES, TIMEOUT, IS_AUTO_SUBMIT,
                                     is_debug))
    try:
        sch.scheduling()
    except KeyboardInterrupt:
        LOG.info("Quit.")
        import sys

        for w in sch.workers:
            w.driver.quit()
        sys.exit()
