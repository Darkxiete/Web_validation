from glob import glob
from argparse import ArgumentParser
from os import chdir, mkdir, rename, walk, getcwd
from subprocess import Popen
from os.path import splitext, exists, join, abspath
from main import ex_cmd

def decrypt(file_path):
    with open(file_path, "rb") as f:
        data = bytearray(f.read())
    with open("_{}".format(file_path), "wb") as f:
        ret = b"\x1f\x8b\x08\x00" + data[22:]
        f.write(ret)



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-p", "--path", dest="path", help="Input file path")

    args = parser.parse_args()

    path = args.path
    abslt_root_path = abspath(path)

    sbin_path = getcwd()

    chdir(abslt_root_path)
    tgz_list = glob("*.tgz")
    for tgz in tgz_list:
        decrypt(tgz)

    db_tgz_list = glob("_*.tgz")
    for _tgz in db_tgz_list:
        # _分割
        num = _tgz[1:].split("_")[0]
        _tgz_name = splitext(_tgz)[0]
        if not exists(_tgz_name):
            mkdir(_tgz_name)
        cmd = "tar -zxvf {} -C {}".format(_tgz, _tgz_name)
        print(cmd)
        # 需要等待子线程结束，要不然下面重命名会找不到文件直接主进程关掉了
        ex_cmd(cmd)
    
        # 重命名
        dot_files_path = join(_tgz_name, "_queryResult_db_")
        chdir(dot_files_path)
        print("change dir to {}".format(dot_files_path))
        ex_cmd("ls result*. | sh -c \"xargs -n1 -i mv {} {}tar\"")
        # Popen("ls |sh -c \"xargs -n1 -i sed -i '1d' {}\"", shell=True).communicate()
        ex_cmd("ls result*.tar | sh -c \"xargs -n1 -i tar -zxvf {}\"")
        ex_cmd("ls _queryResult_db_/result*.txt | sh -c \"xargs -n1 -i cat {} >>../../" + num + ".txt\"")
        print("change dir to {}".format(abslt_root_path))
        chdir(abslt_root_path)

