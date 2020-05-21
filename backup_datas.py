from main import ex_cmd
from os import  mkdir, makedirs
from os.path import exists, join
import time


def safe_mkdir(path: str) -> None:
		if not exists(path):
			mkdir(path)


def safe_makedirs(path: str) -> None:
		if not exists(path):
			makedirs(path)


if __name__ == '__main__':
	safe_mkdir("backup")
	date = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
	root = join("backup", date)
	safe_makedirs(root)
	for path in (join(root, "1", "datas"), join(root, "2", "datas"), join(root, "3", "datas")):
		safe_makedirs(path)
	ex_cmd("cp -r 2/datas/*.txt backup/{}/2/datas".format(date))
	ex_cmd("cp -r 2/datas/[!_]*_ret.csv backup/{}/2/datas".format(date))
	ex_cmd("cp -r 3/datas/* backup/{}/3/datas".format(date))