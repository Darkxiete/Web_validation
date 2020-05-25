# 使用前

## 1 首先安装所需要的包

```python
pip install -r requirements.txt 
```

如速度较慢需要更换源，推荐使用清华源

> ## pypi 镜像使用帮助
>
> pypi 镜像每 5 分钟同步一次。
>
> ### 临时使用
>
> ```
> pip install -i https://pypi.tuna.tsinghua.edu.cn/simple some-package
> ```
>
> 注意，`simple` 不能少, 是 `https` 而不是 `http`
>
> ### 设为默认
>
> 升级 pip 到最新的版本 (>=10.0.0) 后进行配置：
>
> ```
> pip install pip -U
> pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
> ```
>
> 如果您到 pip 默认源的网络连接较差，临时使用本镜像站来升级 pip：
>
> ```
> pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
> ```



## 2 安装对应版本的`chromedriver`

由于该脚本中需要驱动chrome进行爬取，因此需要安装与机器上chrome版本一直的chromdriver

> https://npm.taobao.org/mirrors/chromedriver

进入上述网站下载对应系统对应版本的包，替换`2/` `3/`目录下的chromedriver.exe即可

# 使用方法

先清除路径下的`datas`文件

```
python clean_all_datas.py
```



然后运行脚本， 参数`1,2,3,4`是步骤

1. 解密步骤
2. 外网过滤步骤
3. 截图步骤
4. 按host文件分发图片步骤

```python
python main.py -s 1,2,3,4
```

