from shutil import copy
from os.path import join, split, splitext, exists
import pandas as pd
from typing import List
from glob import glob
from os import chdir, mkdir
from argparse import ArgumentParser


def move_files_by_csv(csv_path: str, pics_src_path: str, pics_dir_path: str) -> None:
    df = pd.read_csv(csv_path, names=['hosts'])
    for pic_name in df["hosts"]:
        pic_name = pic_name.replace(":", "_") + ".png"
        pic_src_path = join(pics_src_path, pic_name)
        pic_dst_path = join(pics_dir_path, pic_name)
        pic_dst_path = join("3\\datas\\", pic_dst_path)
        try:
            copy(pic_src_path,pic_dst_path)
            print("copy {} to {}".format(pic_src_path, pic_dst_path))
        except FileNotFoundError as e:
            continue

def move_files_by_csvs(csv_path: List[str], pics_src_path: str) -> None:
    for path in csv_path:
        _, csv_name = split(path)
        csv_path = splitext(csv_name)[0]
        if not exists(csv_path):
            mkdir(csv_path)
        move_files_by_csv(path, pics_src_path, join(".", csv_path))




if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p1", "--csv_path", action="store", dest="csv_path", type=str,
                        help="csv_path.")
    parser.add_argument("-p2", "--pics_src_path", action="store", dest="pics_src_path", type=str,
                        help="pics_src_path.")
    args = parser.parse_args()
    # csv_path = r"E:\Dk\work\sd\涉赌数据处理流程\3\datas"
    # pics_src_path = r"E:\Dk\work\sd\涉赌数据处理流程\3\pics\202005151828"
    csv_path = args.csv_path
    pics_src_path = args.pics_src_path
    pics_dir_path = ""

    csv_files = glob(join(csv_path, "*_ret_hosts.csv"))
    move_files_by_csvs(csv_files, pics_src_path)