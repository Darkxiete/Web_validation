import logging
import os.path
from os.path import exists, join, splitext
import time
import pandas as pd
import re
from collections import Counter
from functools import reduce
import sys
from urllib.parse import urlparse, unquote
from subprocess import PIPE, Popen
import json
from typing import Dict, Tuple
from argparse import ArgumentParser

LOG_LEVEL_DICT = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARNING,
    'error': logging.ERROR
}

CHINESE = re.compile(r'[\u4E00-\u9FA5]+')
NOCHINESE = re.compile(r"[^\u4E00-\u9FA5]+?\s")


class Logger:

    def __init__(self, logger: str, is_debug=False):
        """
        指定保存日志的文件路径，日志级别，以及调用文件
            将日志存入到指定的文件中
        :param logger: 日志名
        """
        # 创建一个logger
        self.logger = logging.getLogger(logger)
        # self.logger.setLevel(LOG_LEVEL_DICT[log_level])
        if is_debug:
            logger_level = logging.DEBUG
        else:
            logger_level = logging.INFO
        # 相对路径，可以换成绝对路径
        self.path = './Logs'
        if not exists(self.path):
            os.mkdir(self.path)
        # 创建一个handler，用于写入日志文件
        self.rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
        self.logger.setLevel(logger_level)
        log_name = join(self.path, self.rq + '.log')
        fh = logging.FileHandler(log_name)
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def getlog(self):
        return self.logger, self.rq


def pad_url(url):
    """
    1.url补全http
    2.url填充可能域名
    :param url:
    :return:
    """
    ret = []
    url_struct = url.split('.')
    if len(url_struct) == 2:
        ret.append('http://' + url if 'http' not in url else url)
        ret.append('http://www.' + url if 'http' not in url else url)
    if len(url_struct) == 3:
        ret.append('http://' + url if 'http' not in url else url)
        ret.append('http://www.' + '.'.join(url_struct[1:]) if 'http' not in url else 'www.' + '.'.join(url_struct[1:]))
    return ret


def is_chinese_web(row) -> int:
    """
    是否是中文网站
    1. 是否包含中文
    2. 有login且外文词数不多
    :param row:
    :return:
    """
    fulltext = str(row['title']) + str(row['keywords']) + str(row['description']) + str(row['form_data'])
    if row['title'] == '' and row['keywords'] == '' and row['description'] == '':
        return 1
    elif CHINESE.findall(fulltext):
        return 1
    elif len(NOCHINESE.findall(fulltext)) < 400:
        return 1
    else:
        return 0


def word_filter(file_path: str) -> pd.DataFrame:
    """
    对爬取结果进行关键词打标，只打标不过滤，关键词的作用需要考究
    关键词文件，以\r\n分割
    :param file_path:    爬虫结果文件
    :return:        打标结果 DataFrame
    """

    def mark(row, words_list: list) -> Dict[str, int]:
        """
        返回的标记为一个字典，字典形如{"keywords1": 1, "keywords1": 2}
        :param row:
        :return:
        """
        # 关键词正则，终于统计词频
        p1 = re.compile(r'{}'.format('|'.join([words.strip() for words in words_list])))
        res = []
        res.extend(p1.findall(str(row['title'])))
        res.extend(p1.findall(str(row['description'])))
        res.extend(p1.findall(str(row['keywords'])))
        res = dict(Counter(res))
        return res if res != {} else {}

    def combine_word(x, y):
        x.update(y)
        return x

    def count_input(row: str) -> int:
        """
        没有密码框的返回0，只有有密码框的时候才计算框的数量
        :param row: '[int(是否有密码框，有则1无则0), List[str](所有框的name，包括text框和password框)]'
        :return:
        """
        if isinstance(row, str):
            pwd_nums, keys = json.loads(row)
            if pwd_nums:
                return len(keys)
            else:
                return 0
        else:
            return 0

    # 读取关键词
    with open('./keywords/neg_keywords.txt', encoding='utf8') as f:
        keywords = f.read()
    neg_words = keywords.split('\n')
    with open('./keywords/pos_keywords.txt', encoding='utf8') as f:
        keywords = f.read()
    pos_words = keywords.split('\n')
    # 读取爬虫结果
    df = pd.read_csv(file_path,
                     sep='\t',
                     names=['referer', 'icp', 'title', 'keywords', 'description', 'names'],
                     dtype={'referer': 'str', 'icp': 'str', 'title': 'str', 'keywords': 'str', 'description': 'str',
                            'names': 'str'})
    assert len(df != 0), "{}文件为空".format(file_path)
    df['icp_label'] = df.apply(lambda row: 1 if 'ICP' in str(row['icp']).upper() else 0, axis=1)
    df['neg_words_label'] = df.apply(lambda row: mark(row, neg_words), axis=1)
    df['pos_words_label'] = df.apply(lambda row: mark(row, pos_words), axis=1)
    df["input_num"] = df["names"].apply(lambda row: count_input(row))
    return df


def ip_addr(ip):
    cmd = 'curl "http://ip.taobao.com/service/getIpInfo.php?ip={}" -s'.format(ip)
    ret, err = Popen(cmd, stdout=PIPE, shell=True).communicate()
    country = ''
    try:
        country = json.loads(ret.decode())['data']['country']
    except:
        pass
    return country


def pre_filter(df: pd.DataFrame, logger: Logger = None, writer=None) -> pd.DataFrame:
    """
    爬虫前的预处理，4步
    输入数据是读取的源文件

    1. ip过滤。过滤ip中以115的开头的
    2. host过滤。过滤掉host中包含政府，教育机构后缀的
    3. URL规则规律。过滤掉域名中不包含数字的
    4. urldecode。将密码urldecode一下
    :param df:
    :return:
    """
    if logger:
        logger.info("=" * 30)
        logger.info("爬取前的数据预处理，输入数据共{}条".format(len(df)))
    if 'IPADDR' not in df.columns:
        df = df[-(df['dst_ip_id'].str.contains('^1156') == True)]
        if logger:
            logger.info("过滤掉ip以1156开头的数据后，剩余{}条".format(len(df)))
    df = df[-df['HOST'].str.contains('org|edu|mil|gov|biz')]
    if logger:
        logger.info("过滤掉政府、教育、组织机构网站数据后，剩余{}条".format(len(df)))
    df = df[df['HOST'].str.contains('\d')]
    if logger:
        logger.info("过滤URL中不包含数字的数据，剩余{}条".format(len(df)))
    df['PASSWORD'] = df['PASSWORD'].apply(lambda x: unquote(str(x)))
    if logger:
        logger.info("=" * 30)
    return df


def uni_format(file_path: str, file_from: str, id:str, logger: Logger = None, writer=None) -> None:
    """
    读取源文件，输入index, host, referer 三列，作为爬虫数据的输入文件。
    会生成两个文件在本地：
        1. `{file_from}.tsv`: 对全文原文本文件预处理之后的文件，
        2. `referers.tsv`: 预处理之后REFERER去重生成的文件，给爬虫使用
    :param file_path:
    :param file_from:
    :return:
    """
    p_http = re.compile("(https?|ftp|file)[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")
    p_no_http = re.compile("[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")
    def _urlparse(x):
        ret1 = p_http.findall(x)
        ret2 = p_no_http.findall(x)
        if ret1 != []:
            try:
                return urlparse(x).netloc
            except:
                return ""
        elif ret2 != []:
            try:
                x = "http://" + x 
                return urlparse(x).netloc
            except:
                return ""
        else:
            return ""
    
    if file_from == 'wu':
        col = ['AUTH_ACCOUNT', 'dst_ip', 'src_ip', 'REFERER', 'form_data', 'USERNAME', 'PASSWORD', 'code', 'src_ip_id',
               'dst_ip_id', 'CAPTURE_TIME']
    elif file_from == 'tang':
        col = ['AUTH_ACCOUNT', 'CAPTURE_TIME', 'HOST', 'REFERER', 'TITLE', 'IPADDR', 'USERNAME', 'PASSWORD']
    else:
        sys.exit(-1)
    with open(file_path, encoding='utf8', errors='ignore') as f:
        df = pd.read_csv(f,
                         sep='\t',
                         error_bad_lines=False,
                         names=col,
                         dtype=dict([(c, 'str') for c in col]))
        if logger:
            logger.info("读取数据{}，剩余{}条".format(file_path, len(df)))
        if writer:
            writer.write("读取数据共\t{}".format(len(df)))
            writer.write("\n")
            writer.flush()
    df = df[-df['REFERER'].isnull()]
    if logger:
        logger.info("过滤掉不包含REFERER字段的数据后，剩余{}条".format(len(df)))
    df = df[-df['USERNAME'].isnull()]
    if logger:
        logger.info("过滤掉不包含USERNAME字段的数据后，剩余{}条".format(len(df)))
    if 'HOST' not in df.columns:
        df['HOST'] = df['REFERER'].apply(lambda x: _urlparse(x))
    if 'code' not in df.columns:
        df['code'] = ''
    # 不能只去最新的，可能不对
    # df = df.groupby('HOST').apply(lambda _df: _df[_df['CAPTURE_TIME'] == _df['CAPTURE_TIME'].max()])
    # TODO 刘反馈 head(4) 没得用 盐城那批数据测试一下
    df = df.groupby('HOST', as_index=False).apply(lambda _df: _df.sort_values('CAPTURE_TIME', ascending=False).head(4))
    if logger:
        logger.info("每个HOST下账号密码只取近期前4条，剩余{}条".format(len(df)))
    df = pre_filter(df, logger, writer)

    web_info_df = df[['HOST', 'dst_ip', 'src_ip', 'REFERER', 'form_data', 'AUTH_ACCOUNT', 'CAPTURE_TIME', 'USERNAME',
        'PASSWORD', 'code']]
    web_info_df.to_csv('./tmp/{}_{}.tsv'.format(id, file_from), sep='\t', encoding='utf8', index=False)
    if logger:
        logger.info("生成网站基本信息文件，路径：{},共{}条".format('./tmp/{}_{}.tsv'.format(id, file_from), len(web_info_df)))

    referer_df = df[['REFERER']].drop_duplicates()
    referer_df.to_csv('./tmp/{}_referers.tsv'.format(id), sep='\t', encoding='utf8', index=False)
    if logger:
        logger.info("生成referers文件，路径：{},共{}条".format('./tmp/{}_referers.tsv'.format(id), len(referer_df)))


def concat(res_file_path: str, input_file_path: str, output_file_path: str,
           LOG: Logger = None,
           is_filter_by_word: bool = False,
           is_filter_by_input: bool = False,
           is_filter_by_country: bool = False,
           writer = None
           ) -> None:
    """
    使用爬虫结果文件关联上原始文件，并将结果保存在输出路径中
    过滤逻辑：
        1. 正向关键词逻辑： 用于防止删除了正确的数据。有的页面其可能没有捕获到input框，但是使用浏览器访问的时候是有input框的，这个时候如果关键词带后台，则保留
        2. 反向关键词逻辑： 用于删除数据。凡是命中返现关键词的一律删除
        3. input框逻辑： 用于筛选出包含密码框并且input框数量 > 1（后改为 > 0，没有捕获到密码的网站可以考虑填12345等默认密码） 的网站。如网站爬虫没有捕获input框但是包含正向关键词则保留数据
    :param res_file_path:        爬虫爬取的结果文件
    :param input_file_path:      全文输出文件
    :param output_file_path:     输出文件路径
    :param LOG:                  日志
    :param is_filter_by_word:    是否使用关键词过滤
    :param is_filter_by_input:   是否使用input框过滤
    :param is_filter_by_country: 是否使用`是否为中文网站`这一标签来过滤
    :return:
    """
    # res_file
    if LOG is None:
        LOG, _ = Logger("test").getlog()
    mark_file = word_filter(res_file_path)
    assert len(mark_file) != 0, "文件: {} 过滤后为空！".format(mark_file)
    LOG.info("爬取结果文件共{}条".format(len(mark_file)))

    # TODO 对form_data进行去重。去重处理操作
    input_file = pd.read_csv(input_file_path, sep='\t')
    assert len(input_file) != 0, "输入文件为空，请检查输入文件" + input_file_path
    LOG.info("输入文件共{}条".format(len(input_file)))

    df = pd.merge(input_file, mark_file,
                  how='left', left_on='REFERER', right_on='referer')
    df.sort_values(['HOST', 'CAPTURE_TIME'], inplace=True, ascending=False)
    assert len(df) != 0, "爬取结果与输入文件合并后为空，请检查 {} 与 {}".format(res_file_path, input_file_path)
    LOG.info("合并后文件共{}条".format(len(df)))

    # 过滤掉没有爬取成功的
    total = len(df)
    df = df[-df["neg_words_label"].isnull()]
    LOG.info("过滤掉未爬取成功的数据{}条".format(total - len(df)))
    # 必须字符串化， 要不然后面无法去重
    df['neg_words_label'] = df['neg_words_label'].astype('str')
    df['pos_words_label'] = df['pos_words_label'].astype('str')

    _, file_name = get_file_name(output_file_path)
    print(file_name, output_file_path)
    df.drop_duplicates().to_csv("./datas/_{}.csv".format(file_name), sep='\t', index=False)
    now_num = len(df)
    if is_filter_by_word:
        df = df[df["neg_words_label"] == "{}"]
        LOG.info("反向关键词过滤掉{}条数据".format(now_num - len(df)))
    now_num = len(df)
    if is_filter_by_input:
        df = df[(df["pos_words_label"] != "{}") | (df["input_num"] > 0)]
        LOG.info("通过正向关键词与网站输入框数量过滤掉{}条数据".format(now_num - len(df)))
    now_num = len(df)
    if is_filter_by_country:
        # 中文正则，使用title+keywords+description，剔除全外文网站
        # 该方法存在一种特殊情况，即当title，keywords、description都为空时，此时会判定为外文网站
        df['chinese_web'] = df.apply(lambda row: is_chinese_web(row), axis=1)
        df[df["chinese_web"] == 0].to_csv("./tmp/{}_foreign".format(file_name), sep='\t', index=False)
        df = df[df["chinese_web"] == 1]
        LOG.info("外文网站过滤掉{}条数据".format(now_num - len(df)))
    # 0918 刘要求新增四列数据
    other_cols = ['label', 'remarks', 'capturetime', 'location']
    for col in other_cols:
        df[col] = ''
    now_num = len(df)
    df = df.drop_duplicates()
    LOG.info("去重过滤掉{}条数据".format(now_num - len(df)))
    df.to_csv(output_file_path, sep='\t', index=False)
    LOG.info("最终{}条数据".format(len(df)))
    if writer:
        writer.write("最终数据\t{}".format(len(df)))
        writer.flush()
    df["HOST"].drop_duplicates().to_csv("./datas/{}_hosts.csv".format(file_name), sep='\t', index=False)


def purify(line: str) -> Dict[str, str]:
    """
    过滤
    1. 首先取得form_data字段，不要_QUERY_MATCHTERMS
    2. 凡是form_data字段里带"?", "&"的基本都是网页url，统统不要
    3. value太长的，基本都是md5后的值，也不要
    :param line:
    :return:
    """
    sep = ';'
    data = line.split(';_QUERY_MATCHTERMS')[0].strip("...") \
        .replace('; ', sep).replace('...', sep).replace('##', sep)
    eles = data.split(';')
    eles = filter(lambda x: ('?' not in x) and ('&' not in x) and ('=' in x), eles)
    return dict([tuple(ele.split('=', 1)) for ele in eles])


def is_valid_web(text: str, logger=None) -> bool:
    """
    验证页面有效性
    :param text: 网页内容，
    :param logger: logger
    :return: bool
    """
    pattern = re.compile(
        r"(无法访问|无法找到该页|该网页无法正常运作|找不到文件|无法加载控制器|页面错误|未知错误|页面未找到|请联系接入商|域名已过期|"
        r"没有找到站点|Not Found|404错误|<html><head></head><body></body></html>|访问受限|尚未绑定|page note found|"
        r"HTTP 错误)")
    ret = pattern.findall(text)
    if ret:
        if logger:
            logger.debug("无效网址，Detail：{}".format(ret[0]))
        return False
    else:
        return True


def get_file_name(file_path: str, with_suffix = False) -> Tuple[str, str]:
    """
    file_path like E:\ProgramData\Anaconda3\lib\ntpath.py
    or E://ProgramData//Anaconda3//lib//ntpath.py
    => E:/ProgramData/Anaconda3/lib/ntpath.py
    """
    sep = "/"
    file_path = file_path.replace("\\", sep).replace("//", sep)
    right_sep_index = file_path.rfind(sep)
    dir_path = file_path[:right_sep_index]
    file_name = file_path[right_sep_index + 1:]
    if not with_suffix:
        file_name = file_name[:file_name.rfind(".")]
    return dir_path, file_name




if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p", "--path", action="store", dest="path", help="Path of input file.")
    # 过滤条件
    parser.add_argument("-w", "--word_filter", action="store_true", dest="is_filter_by_word",
                        help="Filter web info via word label.")
    parser.add_argument("-i", "--input_filter", action="store_true", dest="is_filter_by_input",
                        help="Filter web info via input label.")
    args = parser.parse_args()
    path = args.path
    is_filter_by_word = args.is_filter_by_word
    is_filter_by_input = args.is_filter_by_input

    concat(path, './tmp/202005141347_wu.tsv', "./datas/610100_ret.csv",
           is_filter_by_word=is_filter_by_word,
           is_filter_by_input=is_filter_by_input,
           is_filter_by_country=True)
