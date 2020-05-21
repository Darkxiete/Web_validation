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
from typing import Dict
from argparse import ArgumentParser
from typing import List, Tuple
from bs4 import BeautifulSoup

LOG_LEVEL_DICT = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARNING,
    'error': logging.ERROR
}


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
    if ("http://" in url) or ("https://" in url):
        return url
    return "http://" + url


def is_valid_web(text: str) -> Tuple[bool, str]:
    """
    验证页面有效性
    :param text: 网页内容，
    :param logger: logger
    :return: bool
    """
    # TODO 错误细分，有的错误是不需要重试的错误
    # <html><head></head><body></body></html>|
    pattern = re.compile(
        r"(无法访问|无法找到该页|该网页无法正常运作|找不到文件|无法加载控制器|页面错误|未知错误|页面未找到|请联系接入商|域名已过期|"
        r"没有找到站点|Not Found|404错误|访问受限|尚未绑定|page note found|"
        r"HTTP 错误)")
    soup = BeautifulSoup(text, "html5lib")
    ret = pattern.findall(text)
    if ret:
        div = soup.find("div", attrs={"class": "error-code"})
        if div:
            ret = [div.text]
        return False, ret[0]
    else:
        return True, ""


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

    concat(path, './tmp/wu.tsv', "./datas/_result_crawler_dl.csv",
           is_filter_by_word=is_filter_by_word, is_filter_by_input=is_filter_by_input)
