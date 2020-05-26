from os.path import split, splitext, getctime, join, exists
from os import chdir, mkdir, listdir
from subprocess import Popen
from argparse import ArgumentParser
import re
from glob import glob


PyPath = None

def ex_cmd(cmd: str, py_path=None) -> None:
    print(cmd)
    if "python" in cmd and py_path:
        cmd = cmd.replace("python", py_path)
    Popen(cmd, shell=True).communicate()


def get_last_list(file_path):
    dir_list = listdir(file_path)
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
    parser.add_argument("-p", "--path", action="store", dest="path", default="..\\python.exe",
                        help="path of python interruptor")
    args = parser.parse_args()
    steps = args.steps.split(",")
    debug = args.debug
    strict = args.strict
    path = args.path


    assert len(steps) > 0, "指定步骤为空！请正确添加执行步骤，如'1,2,3,4'"

    if '1' in steps:
        print("Step 1 解密")
        ex_cmd("python 1/decrypt.py -p 1/datas", path)
        ex_cmd("mv 1/datas/*.txt 2/datas")

    if '2' in steps:
        print("Step 2 外网过滤")
        chdir("2")
        for file in glob("datas/*.txt"):
            cmd = "python icp_crawler.py -p datas/{} -w -i".format(split(file)[1])
            if debug:
                cmd += " -d"
            if strict:
                cmd += " -c"
            ex_cmd(cmd, path)
        chdir("..")

    if '3' in steps:
        print("Step 3 外网截图")
        ex_cmd("cat 2/datas/*_hosts.csv >> 3/datas/_total_hosts.csv")
        ex_cmd("cat 3/datas/_total_hosts.csv | sort -u > 3/datas/total_hosts.csv")
        ex_cmd("cp 2/datas/*_hosts.csv 3/datas")
        chdir("3")
        for file in glob("datas/*_ret_hosts.csv"):
            file_dir = splitext(file)[0]
            if not exists(file_dir):
                mkdir(file_dir)
        ex_cmd("python crawler.py -p datas/total_hosts.csv", path)
        chdir("..")

    if '4' in steps:
        print("Step 4 分发图片")
        last_pics_path = get_last_list("3/pics")
        ex_cmd("python move_pics_by_csv.py -p1 3\datas -p2 3\pics\{}".format(last_pics_path), path)
