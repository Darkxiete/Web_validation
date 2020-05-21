from glob import glob
from os import chdir

# labels = [
#     "网站首页", "充值界面", "会员管理", "代理管理", "财务报表", "财务管理", "充值提款管理", "其他"
# ]


def get_label(name):
    if "网站" in name:
        return "网站首页"
    elif "充值" in name:
        return "充值界面"
    elif "会员" in name:
        return "会员管理"
    elif "用户" in name:
        return "会员管理"
    elif "代理" in name:
        return "代理管理"
    elif "报表" in name:
        return "财务报表"
    elif "财务" in name:
        return "财务报表"
    elif "提现" in name:
        return "充值提款管理"
    elif "收款" in name:
        return "充值提款管理"
    else:
        return "其他"


if __name__ == '__main__':
    path = r'E:\Dk\work\sd\人工验证\0109\广东佛山-夏天\异常'
    chdir(path)
    pngs = glob("*.png")
    for png in pngs:
        host, name = png.split('.png')[0].split('_')
        label = get_label(name)
        print(f"{host}\t{png}\t{label}")
