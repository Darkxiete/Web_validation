#-*- coding: utf-8 -*-
# #--------------------文件夹图片名称自动读取----------
from os import listdir
import requests
from bs4 import BeautifulSoup
import json
import re
import urllib.parse
import base64
import shutil
from argparse import ArgumentParser
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
global c

def group_Figure(path):
    #-----------------------将识别赌博图片保存到result_figure文件
    with open(r'./tmpResult/result.txt','r+') as hj:
        total_text=hj.readlines()
    f_number1=len(total_text)
    
    path1='.//result_figure//'
    if(f_number1>0):
        for j in range(f_number1):
            shutil.copy(path+total_text[j].strip(),path1+total_text[j].strip())
    #-----------------------将不识别赌博图片保存到rawFigure文件
    with open(r'./tmpResult/fail.txt','r+') as hj1:
        total_text2=hj1.readlines()

    f_number2=len(total_text2)
    path2='.//failfigure//'
    if(f_number2>0):
        for j2 in range(f_number2):
            shutil.copy(path+total_text2[j2].strip(),path2+total_text2[j2].strip())
        #-----------------------将不识别赌博图片保存到rawFigure文件
    with open(r'./tmpResult/noise.txt','r+') as hj2:
        total_text3=hj2.readlines()
    f_number3=len(total_text3)
    path3='.//noiseFigure//'
    if(f_number3>0):
        for j3 in range(f_number3):
            shutil.copy(path+total_text3[j3].strip(),path3+total_text3[j3].strip())



if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p", "--path", action="store", dest="path", required=True, type=str,
                        help="Data path.")
    args = parser.parse_args()
    path = args.path
    f_list1=listdir(path)
    f_number1=len(f_list1)

    h=[]
    for fname in f_list1:
        if fname[-3:]=='png':
           h.append(fname)
    f_number=[xs for xs in range(len(h))]

    #--------------------------获取秘钥-----------------
    import token_ocr
    values =token_ocr.getKey()
    key_list=values.split(',')
    keyValue=key_list[3].split(':')[1]
    #----------------------图片转码-----------------
    
    #清空文件内容
    with open(r"./tmpResult/base64g.txt", "r+")as f100:
        f100.truncate()
    with open(r"./tmpResult/fail.txt", "r+")as f101:
        f101.truncate()
    with open(r"./tmpResult/ocrContent.txt", "r+")as f102:
        f102.truncate()
    with open(r"./tmpResult/result.txt", "r+")as f103:
        f103.truncate()
    with open(r"./tmpResult/noise.txt", "r+")as f104:
        f104.truncate()
    j=0
    for e in h:
        f5=path+e
        with open(f5,"rb") as f3: 
            base64_data=base64.b64encode(f3.read())
        with open(r"./tmpResult/base64g.txt",'ab+') as ff:
            ff.write(base64_data+b'\r\n')
    #-------------正向关键词-----------------
    keywords1=""
    negwords1=""
    with open('./keywords/posKeys.txt', 'r',encoding='utf-8') as fg1:
        for line in fg1.readlines():
            line = line.strip()
            keywords1 += line
    #-------------反向关键词-----------------
    with open('./keywords/negKeys.txt', 'r',encoding='utf-8') as fg2:
        for line in fg2.readlines():
            line = line.strip()
            negwords1 += line
    pattern1 = re.compile(keywords1)
    pattern2 = re.compile(negwords1)
    #--------------------------------------------------
    with open(r"./tmpResult/base64g.txt", 'r') as f:  # 打开文件
        lines = f.readlines() 
    for di in f_number:
    #------------------读取文件----------------
     # 读取所有行
        first_line = lines[di]
        hf={}
        hf['image']=first_line.strip()
        body=urllib.parse.urlencode(hf)
        c=requests.Session()
        url='https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic?access_token='+keyValue
        headers={
        'Content-Type': 'application/x-www-form-urlencoded'}
        c.headers=headers
        c.keep_aliver=False
        c.verify=False
        nk=0
        import re
        while(nk<3):
            try: 
                req=c.post(url,headers=headers,data=body)
                print("processing pic: {}/{}".format(di + 1, len(f_number)))
                contentDict = json.loads(req.text)
       
                word_list = contentDict['words_result']
                feature_dic={}
                a10=str()
                for word_dict in word_list:
                    a10=a10+word_dict['words']+','
                feature_dic[h[di]]=a10
                with open(r"./tmpResult/ocrContent.txt",'a+',encoding='utf-8') as ff2:
                    ff2.write(a10+'\n')
    #-------------------------正则表达式进行文本匹配
                match1 = pattern1.match(a10)
                match2 = pattern2.match(a10)
                if (match1 and not match2):
                    ff2=open(r"./tmpResult/result.txt",'a+')
                    ff2.write(h[di]+'\n')
                    ff2.close()
                else:
                    ff3=open(r"./tmpResult/noise.txt",'a+')
                    ff3.write(h[di]+'\n')
                    ff3.close()
                break
            except:
                nk=nk+1
                if nk==2:
                    with open(r"./tmpResult/fail.txt",'a+',encoding='utf-8') as ff22: 
                        ff22.write(h[di]+'\n')
    
    group_Figure(path)
