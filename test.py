from ex_cmd import ex_cmd


if __name__ == "__main__":
    ex_cmd('TASKKILL /IM chromedriver.exe /F')
    ex_cmd('TASKKILL /IM chromedriver.exe /F', None, True)