from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException, TimeoutException, \
    NoAlertPresentException, NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from threading import Thread
from queue import Queue
from util import Logger, pad_url
from argparse import ArgumentParser
from typing import Tuple
import time
from os.path import join, exists
from os import makedirs
import traceback
import sys
from copy import deepcopy
from queue import Empty
from subprocess import Popen, PIPE

import sys
import os
import os.path
# 引入上层包
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from main import ex_cmd


replace_for_colon = "冒号"


class Scheduler:
    """
    url_queue: 待爬队列
    failed_queue: 失败队列，需要保存失败URL和失败原因
    """

    def __init__(self, file_path):
        self.url_queue = Queue()
        self.timeout_queue = Queue()
        self.failed_queue = Queue()
        self.workers = []
        self.tm_workers = []
        self._init(file_path)
        self.file_path = file_path

    def _init(self, file_path):
        """
        一些初始化任务
        :param file_path: 输入文件路径
        :return:
        """
        pics_path = 'pics'
        data_path = 'datas'
        log_path = 'Logs'
        result_path = join("pics", RQ)
        for path in (pics_path, data_path, log_path, result_path):
            if not exists(path):
                makedirs(path)
        with open(file_path, 'r', encoding='utf8') as f1:
            content = f1.readlines()
        urls = set(url.strip() for url in content)
        for d in urls:
            self.url_queue.put([d, RETRIES])  # 最多重试2次

    def scheduling(self):
        """
        启动爬虫线程
        启动守护线程
        阻塞主线程
        :return:
        """
        workers_info = []

        LOG.debug("[Scheduler]SchedulerLength of queue: {}".format(self.url_queue.qsize()))

        LOG.debug("[Scheduler]Spawning the Workers.")
        for i in range(THREADS_NUM):
            worker = ThreadCrawl(self.url_queue, self.failed_queue, self.timeout_queue, i + 1)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
            workers_info.append((worker, i + 1, 'ThreadCrawl'))

        for i in range(THREADS_NUM - 2):
            worker = TimeoutCrawl(self.url_queue, self.failed_queue, self.timeout_queue, i + 1)
            worker.daemon = True
            worker.start()
            self.tm_workers.append(worker)
            workers_info.append((worker, i + 1, 'TimeoutCrawl'))

        self.d = Daemon(self.file_path, workers_info, self.url_queue, self.failed_queue, self.timeout_queue)
        self.d.daemon = True
        self.d.start()

        self.url_queue.join()
        self.timeout_queue.join()
        self.failed_queue.join()


class ThreadCrawl(Thread):
    """
    截图线程，Selenium默认加载逻辑为“阻塞加载”
    """

    def __init__(self, queue_1, queue_2, queue_3, tid, page_load_strategy="normal"):
        Thread.__init__(self)
        self.url_queue = queue_1
        self.failed_queue = queue_2
        self.timeout_queue = queue_3
        self.tid = tid
        self.status = 'Launching...'
        self._exit_code = 0
        self._exit_info = ''
        self._exception = ''
        self._exc_traceback = ''
        self.page_load_strategy = page_load_strategy

    def __del__(self):
        self.driver.quit()

    def _init(self) -> None:
        """
        设置浏览器属性并打开浏览器
        设置driver属性
        :return:
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.binary_location = os.path.abspath(os.path.join(os.path.dirname(__file__),  os.path.pardir, os.path.pardir, "chrome", "chrome.exe"))
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("disable-web-security")
        # 是否打开浏览器
        if is_headless:
            chrome_options.add_argument("--headless")
        # none表示将br.get方法改为非阻塞模式，在页面加载过程中也可以给br发送指令，如获取url，pagesource等资源。
        desired_capabilities = deepcopy(DesiredCapabilities.CHROME)  # 修改页面加载策略
        desired_capabilities["pageLoadStrategy"] = self.page_load_strategy
        self.driver = webdriver.Chrome(chrome_options=chrome_options,
                                       desired_capabilities=desired_capabilities)
        self.status = 'Started.'
        self.driver.implicitly_wait(0.5)
        self.driver.set_page_load_timeout(TIMEOUT)
        self.driver.set_script_timeout(20)

    def _execute_js(self, js: str) -> None:
        try:
            self.driver.execute_script(js)
        except TimeoutException:
            self.driver.close()

    def _gen_tab(self) -> Tuple[str, str]:
        """
        生成次Tab，并切换到主Tab，防止Tab关闭了浏览器退出
        :return:
        """
        try:
            main_win = self.driver.current_window_handle
            all_win = self.driver.window_handles
        except NoSuchWindowException:
            all_win = self.driver.window_handles
            main_win = all_win[0]
            self.driver.switch_to.window(main_win)
        sub_win = ""
        if len(all_win) == 1:
            self.driver.execute_script('window.open("about:blank")')
            all_win = self.driver.window_handles
            sub_win = all_win[1]
            self.driver.switch_to.window(sub_win)
            # 还是定位在main_win上的
            self.driver.switch_to.window(main_win)
        return main_win, sub_win

    def _is_reachable(self, url, count) -> bool:
        """
        判断网站是否可访问
        是否在规定的时间内加载完 全页面元素， 是则为可访问，不是则为不可访问
        :param url:
        :param count:
        :return:
        """
        LOG.debug("[Worker]Worker: {} REQUESTING the url: {}, life: {}.".format(self.tid, url, count))
        try:
            url = pad_url(url)
            self.driver.get(url)
            # 可能有Alert弹窗
            # TODO 其他弹窗
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
            return True
        except TimeoutException:
            return False

    def run(self) -> None:
        """
        最重要的问题就是`NoSuchWindowException`
        整个运行过程都要防止没有窗口的问题
        :return:
        """
        import sys
        try:
            self._init()
            while True:
                LOG.debug("[Worker]Worker: {} GETTING web info".format(self.tid))
                url, count = self.url_queue.get()
                LOG.debug("[Worker]Worker: {} Length of queue: {}".format(self.tid, self.url_queue.qsize()))
                LOG.debug("[Worker]Worker: {} URL: {}".format(self.tid, url))

                # 两个Tab，main_win是主窗口，用来验证网站, sub_win是副窗口，用来打印参数
                main_win, _sub_win = self._gen_tab()

                # LOG.debug("[Worker]Worker: {} REQUESTING the url.".format(self.tid))
                if not self._is_reachable(url, count):
                    LOG.debug("[Worker]Worker: {} URL unreachable: {}, count: {}".format(self.tid, url, count))
                    if count > 0:
                        LOG.debug("[Worker]Worker: {} PUT url into url queue queue.".format(self.tid))
                        self.url_queue.put((url, count - 1))
                    if count == 0:
                        LOG.debug("[Worker]Worker: {} PUT url into timeout queue.".format(self.tid))
                        self.timeout_queue.put(url)
                    self._execute_js("window.stop()")  # 超时会导致窗口关闭
                    self.url_queue.task_done()
                    continue
                # 保存图片的时候需要把当前操作系统非法文件名符号剔除掉
                #url_name = url if ":" not in url else url.split(":")[0]
                url_name = url.replace(":", replace_for_colon)
                save_path = 'pics/{}/{}.png'.format(RQ, url_name.split('/')[0])
                self.driver.get_screenshot_as_file(save_path)
                self.url_queue.task_done()
                LOG.debug("[Worker]Worker: {} URL:{} is done.".format(self.tid, url))
        except Exception as e:
            LOG.debug("[Worker]Worker: {} is dead, error info: {}".format(self.tid, repr(e)))
            self._exit_code = 1
            self._exception = e
            self._exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            LOG.debug("[Worker]Worker: {} Traceback: {}.".format(self.tid, self._exc_traceback))
            self.failed_queue.put((url, repr(e), self._exc_traceback))
            self.url_queue.task_done()
            self.driver.quit()

    def exit(self):
        self.driver.quit()


class TimeoutCrawl(ThreadCrawl):
    """
    截图线程， selenium加载逻辑变为非阻塞式加载
    """

    def __init__(self, queue_1, queue_2, queue_3, tid):
        ThreadCrawl.__init__(self, queue_1, queue_2, queue_3, tid, "none")

    def _is_reachable(self, url, count) -> bool:
        """
        判断网站是否可访问
        1. 超时->不可访问
        2. 含有`error-code`->不可访问
        :param url:
        :param count:
        :return:
        """
        LOG.debug("[Worker]Worker: {} REQUESTING the url: {}, life: {}/{}.".format(self.tid, url, count, RETRIES))
        try:
            url = pad_url(url)
            self.driver.get(url)
            try:
                # 1. 含有error-code
                self.driver.find_element_by_xpath('//*[@class="error-cod1e"]')
                return False
            except NoSuchElementException:
                pass
            # 可能有Alert弹窗
            # TODO 其他弹窗
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
            return True
        except TimeoutException:
            # 2. 超时
            return False

    def run(self) -> None:
        import sys
        try:
            self._init()
            while True:
                LOG.debug("[TM-Worker]Worker: {} GETTING web info".format(self.tid))
                url = self.timeout_queue.get()
                LOG.debug("[TM-Worker]Worker: {} Length of queue: {}".format(self.tid, self.timeout_queue.qsize()))
                LOG.debug("[TM-Worker]Worker: {} URL: {}".format(self.tid, url))

                if self._is_reachable(url, 0):
                    time.sleep(RENDER_TIME)
                self._execute_js("window.stop()")
                save_path = 'pics/{}/{}.png'.format(RQ, url.split('/')[0])
                self.driver.get_screenshot_as_file(save_path)
                self.timeout_queue.task_done()
        except Exception as e:
            LOG.debug("[TM-Worker]Worker: {} is dead, error info: {}".format(self.tid, repr(e)))
            self._exit_code = 1
            self._exception = e
            self._exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            LOG.debug("[TM-Worker]Worker: {} Traceback: {}.".format(self.tid, self._exc_traceback))
            self.failed_queue.put((url, repr(e), self._exc_traceback))
            self.timeout_queue.task_done()
            self.driver.quit()


class Daemon(Thread):
    def __init__(self, path, workers_info, queue_1, queue_2, queue_3):
        Thread.__init__(self)
        self.workers_info = workers_info
        self.url_queue = queue_1
        self.failed_queue = queue_2
        self.timeout_queue = queue_3
        # 较为详细的出错信息，包括URL、error、trackback
        self.output_file = open("pics/{}/failed_url.csv".format(RQ), "w")
        # 简单的出错信息，仅包括URL
        self.output_file_1 = open(path + "_failed.csv", "w")

    def run(self):
        while True:
            time.sleep(5)
            LOG.info("=" * 60)
            LOG.info("[Daemon]URL_QUEUE: {} , unfinished task: {}".format(self.url_queue.qsize(),
                                                                          self.url_queue.unfinished_tasks))
            LOG.info("[Daemon]FAILED_QUEUE: {}, unfinished task: {}".format(self.failed_queue.qsize(),
                                                                            self.failed_queue.unfinished_tasks))
            LOG.info("[Daemon]TIMEOUT_QUEUE: {}, unfinished task: {}".format(self.timeout_queue.qsize(),
                                                                             self.timeout_queue.unfinished_tasks))
            LOG.info("-" * 60)

            for w, tid, _type in self.workers_info:
                if _type == "ThreadCrawl":
                    LOG.info(
                        "[Daemon]Worker: {} Status: {}, Strategy: {}".format(tid, 'Alive' if w.is_alive() else 'Dead',
                                                                             w.page_load_strategy))
                else:
                    LOG.info(
                        "[Daemon]TM-Worker: {} Status: {}, Strategy: {}".format(tid,
                                                                                'Alive' if w.is_alive() else 'Dead',
                                                                                w.page_load_strategy))

            for w, tid, _type in self.workers_info:
                if not w.is_alive():
                    LOG.debug("[Daemon] {}-Worker: {} is dead, spawn a new thread instead.".format(_type, tid))
                    self.workers_info.remove((w, tid, _type))
                    if _type == "ThreadCrawl":
                        w = ThreadCrawl(self.url_queue, self.failed_queue, self.timeout_queue, tid)
                    else:
                        w = TimeoutCrawl(self.url_queue, self.failed_queue, self.timeout_queue, tid)
                    w.daemon = True
                    w.start()
                    self.workers_info.append((w, tid, _type))
            LOG.info("=" * 60)
            try:
                url, error, track = self.failed_queue.get(block=False)  # 阻塞队列
                self.output_file.write("\t".join([url, error, track]) + "\n")
                self.output_file.flush()
                self.output_file_1.write(url + "\n")
                self.output_file_1.flush()
                self.failed_queue.task_done()
            except Empty:
                pass


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p", "--path", action="store", dest="path", required=True, type=str,
                        help="Data path.")
    parser.add_argument("-n", "--thread_num", action="store", dest="thread_num", default=5, type=int,
                        help="Number of validator threads.")
    parser.add_argument("-r", "--retries", action="store", dest="retries", default=3, type=int,
                        help="Number of retry times.")
    parser.add_argument("-tm", "--timeout", action="store", dest="timeout", default=80, type=int,
                        help="Timeout.")
    parser.add_argument("-rt", "--render_time", action="store", dest="render_time", default=80, type=int,
                        help="Render time.")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug", help="Debug mode.")
    parser.add_argument("-s", "--test", action="store_true", dest="test", help="Using test data.")
    parser.add_argument("-l", "--headless", action="store_true", dest="headless", help="Cast headless mode.")

    args = parser.parse_args()
    path = args.path
    THREADS_NUM = args.thread_num
    RETRIES = args.retries
    TIMEOUT = args.timeout
    RENDER_TIME = args.render_time
    is_debug = args.debug
    is_test = args.test
    is_headless = args.headless
    RUN_PATH = sys.path[0]

    LOG, RQ = Logger('CrawlerLog', is_debug).getlog()

    sch = Scheduler(path)
    try:
        sch.scheduling()
    except KeyboardInterrupt:
        LOG.info("Quit.")
    finally:
        LOG.info('Done.')
        for w, _, _ in sch.d.workers_info:
            w.exit()  # TODO 非阻塞退出
        ex_cmd('TASKKILL /IM chromedriver.exe /F', None, True)
