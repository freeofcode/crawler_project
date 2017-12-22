# -*- coding: utf-8 -*-


from scrapy.cmdline import execute

import sys
import os

# #将系统当前目录设置为项目根目录
# #os.path.abspath(__file__)为当前文件所在绝对路径

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
execute(["scrapy", "crawl" , "jobbole"])
# execute(["scrapy", "crawl" , "zhihu"])
