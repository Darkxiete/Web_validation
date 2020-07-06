from os.path import split, splitext, getctime, join, exists, isfile
from os import chdir, mkdir, listdir
from subprocess import Popen
from argparse import ArgumentParser
import re
from glob import glob

from subprocess import Popen
import os.path
from copy import deepcopy


bin_env = os.path.abspath(os.path.join(os.path.dirname(__file__), "bin")) + ";"
sys32_env = r"C:\Windows\system32;"
GLOBAL_ENV = os.environ.copy()
GLOBAL_ENV["PATH"] = GLOBAL_ENV["PATH"].replace(bin_env, "").replace(sys32_env, "")

org_env = deepcopy(GLOBAL_ENV)
no32_env = deepcopy(GLOBAL_ENV)

if sys32_env not in org_env["PATH"]:
    org_env["PATH"] = sys32_env + org_env["PATH"]
if bin_env not in no32_env["PATH"]:
    no32_env["PATH"] = bin_env + no32_env["PATH"]


def ex_cmd(cmd: str, py_path=None, use_system_path: bool=False) -> None:
    print(cmd)
    if "python" in cmd and py_path:
        cmd = cmd.replace("python", py_path)
    _env = org_env if use_system_path else no32_env
    Popen(cmd, shell=True, env=_env).communicate()


def get_last_list(file_path):
    dir_list = [d for d in listdir(file_path) if not isfile(d)]
    if not dir_list:
        return
    else:
        # 注意，这里使用lambda表达式，将文件按照最后修改时间顺序升序排列
        # os.path.getmtime() 函数是获取文件最后修改时间
        # os.path.getctime() 函数是获取文件最后创建时间
        dir_list = sorted(dir_list, key=lambda x: getctime(join(file_path, x)), reverse=True)
        return dir_list[0]


if __name__ == '__main__':
    """
    1. 进入1 目录，python decpyt.py -p 1/datas
    2. 进入2 目录，python icp_crawler.py -p 2/datas3
    3. 汇总所有hosts, python crawler.py  
    4. 分发图片, python move_pics_by_csv.py 
    """
    parser = ArgumentParser()
    parser.add_argument("-s", "--steps", action="store", dest="steps", required=True, type=str,
                        help="执行步骤，以逗号划分")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug",
                        help="debug mode")
    parser.add_argument("-t", "--strict", action="store_true", dest="strict",
                        help="strict filter mode")
    parser.add_argument("-w", "--filter_by_neg_word", action="store_true", dest="filter_by_neg_word",
                        help="filter by negative word")
    parser.add_argument("-p", "--path", action="store", dest="path", default=os.path.abspath(os.path.join(os.path.dirname(__file__),  os.path.pardir, "Python3.7.6", "python.exe")),
                        help="path of python interruptor")
    parser.add_argument("-l", "--headless", action="store_true", dest="headless",
                        help="headless mode")
    args = parser.parse_args()
    steps = args.steps.split(",")
    debug = args.debug
    strict = args.strict
    filter_by_neg_word = args.filter_by_neg_word
    path = args.path
    is_headless = args.headless


    assert len(steps) > 0, "指定步骤为空！请正确添加执行步骤，如'1,2,3,4'"

    if '1' in steps:
        assert get_last_list("1/datas") is not None, "步骤1没有输入文件"
        print("Step 1 解密")
        ex_cmd("python 1/decrypt.py -p 1/datas", path)
        ex_cmd("mv 1/datas/*.txt 2/datas")

    if '2' in steps:
        assert get_last_list("2/datas") is not None, "步骤2没有输入文件"
        print("Step 2 外网过滤")
        chdir("2")
        for file in glob("datas/*.txt"):
            cmd = "python icp_crawler.py -p datas/{} -i".format(split(file)[1])
            if debug:
                cmd += " -d"
            if strict:
                cmd += " -c"
            if filter_by_neg_word:
                cmd += " -w"
            ex_cmd(cmd, path)
        chdir("..")

    if '3' in steps:
        print("Step 3 外网截图")
        assert get_last_list("2/datas") is not None, "步骤3没有输入文件"
        ex_cmd("cat 2/datas/*_hosts.csv >> 3/datas/_total_hosts.csv")
        ex_cmd("cat 3/datas/_total_hosts.csv | sort -u > 3/datas/total_hosts.csv")
        ex_cmd("cp 2/datas/*_hosts.csv 3/datas")
        chdir("3")
        for file in glob("datas/*_ret_hosts.csv"):
            file_dir = splitext(file)[0]
            if not exists(file_dir):
                mkdir(file_dir)
        cmd = "python crawler.py -p datas/total_hosts.csv"
        if is_headless:
        	cmd += " -l"
        ex_cmd(cmd, path)
        chdir("..")

    if '4' in steps:
        print("Step 4 网站图片分类")
        last_pics_path = get_last_list("3/pics")
        assert last_pics_path is not None, "{}不是可用路径，请检查第三步截图是否成功，如成功请按host文件名在3\\pics路径下创建同名文件".format(last_pics_path)
        chdir("4")
        ex_cmd(r"python ocr.py -p {}".format("../3/pics/{}/".format(last_pics_path)))
        chdir("..")

    if '5' in steps:
        print("Step 5 分发图片")
        last_pics_path = get_last_list("4/result_figure")
        assert last_pics_path is not None, "{}不是可用路径，请检查第三步截图是否成功，如成功请按host文件名在3\\pics路径下创建同名文件".format(last_pics_path)
        ex_cmd(r"python move_pics_by_csv.py -p1 3/datas -p2 4/result_figure".format(last_pics_path), path)
        ex_cmd(r"cp 2/datas/[!_]*_ret.csv ret")

