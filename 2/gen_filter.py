import pandas as pd
from argparse import ArgumentParser
from glob import glob
from os import chdir
from util import purify
from typing import Dict


replace_for_colon = "冒号"


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p1", "--pic_dir", action="store", dest="pic_dir", type=str, required=True,
                        help="Pictures path.")
    parser.add_argument("-p2", "--file_path", action="store", dest="file_path", type=str, required=True,
                        help="Excel path.")
    parser.add_argument("-t", "--file_type", action="store", dest="file_type", type=str,
                        choices=["excel", "csv"], default='csv',
                        help="File type.")
    parser.add_argument("-b", "--by", action="store", dest="by", type=str,
                        choices=["pics", "txt"], default='pics',
                        help="Filter condition.")

    args = parser.parse_args()
    pic_dir = args.pic_dir
    file_path = args.file_path
    file_type = args.file_type
    by = args.by

    file_path = file_path.replace("\\", "/")
    file_name, file_dir = file_path[::-1].split("/", 1)
    file_dir = file_dir[::-1]
    file_name = file_name[::-1]
    chdir(file_dir)
    if file_type == "excel":
        df = pd.read_excel(file_name)
    elif file_type == "csv":
        df = pd.read_csv(file_name, sep="\t")
    else:
        import sys

        print("Unsupported data format!")
        sys.exit(-1)

    chdir(pic_dir)
    if by == "pics":
        pic_list = glob("*.png")
        host_list = [pic[:-4].replace(replace_for_colon, ":") for pic in pic_list]
    elif by == "txt":
        host_df = pd.read_csv("labeled.txt", sep='\t', header=None)
        host_list = host_df[0].drop_duplicates().tolist()
    else:
        import sys
        print("Unsupported p1 format!")
        sys.exit(-1)
    _df = df[df["HOST"].isin(host_list)]
    print(len(_df))
    _df["kvs"] = _df["form_data"].apply(lambda row: str(purify(row)))
    ret = _df.groupby(["HOST", "REFERER", "kvs"]).apply(lambda d: d.head(1)).reset_index(drop=True)
    chdir(file_dir)
    ret.to_csv("filtered.csv", index=False, sep="\t")
