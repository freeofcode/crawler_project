# -*- coding: utf-8 -*-


from scrapy.cmdline import execute

import sys
import os

#将系统当前目录设置为项目根目录
#os.path.abspath(__file__)为当前文件所在绝对路径
#os.path.dirname为文件所在目录
#H:\CodePath\spider\ArticleSpider\main.py
#H:\CodePath\spider\ArticleSpider
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#执行命令，相当于在控制台cmd输入改名了
execute(["scrapy", "crawl" , "jobbole"])



if __name__ == '__main__':
    execute(["scrapy", "crawl", "jobbole"])
    pass