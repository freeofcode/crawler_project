# -*- coding: utf-8 -*-
# -*-coding:gbk-*-
import scrapy
import datetime
from scrapy.http import Request
from urllib import parse
import re
# from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from ArticleSpider.items import JobBoleArticleItem, ArticleItemLoader
from ArticleSpider.utils.common import get_md5
import re
class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    handle_httpstatus_list = [404]

    def __init__(self):
        self.fail_urls = []
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)

    def handle_spider_closed(self, spider, reason):
        self.crawler.stats.set_value("failure_urls", ",".join(self.fail_urls))

    def parse(self, response):

        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value("failure_url")

        post_nodes = response.css("#archive .floated-thumb .post-thumb a")
        # print(post_nodes)
        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first("")
            post_url = post_node.css("::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url":image_url}, callback=self.parse_detail)
            # yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url": image_url})
            # break
        next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if next_url:
            yield Request(url=next_url, callback=self.parse)

    def parse_detail(self, response):
        # 提取文章的具体字段
        article_item = JobBoleArticleItem()
        # re_selector = response.xpath('//div[@class="entry-header"]/h1/text()').extract_first("")
        # create_data = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()[0].strip().replace(".", "").strip()
        # praise_num = response.css(".vote-post-up h10::text").extract()[0]
        # fav_nums = response.css(".bookmark-btn::text").extract()[0]
        # match_re = re.match(".*?(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums=0
        #
        # comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0]
        # match_re = re.match(".*?(d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        #
        # content = response.css("div.entry").extract()[0]
        #
        # tag_list = response.css("p.entry-meta-hide-on-mobile a::text").extract()
        # # tag_list = [element for element in tag_list if not element.strip().endswitch("评论")]
        # tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        # tags = ".".join(tag_list)
        #
        # title = response.css(".entry-header h1::text").extract()[0]
        # create_date = response.css("p.entry-meta-hide-on-mobile::text").extract()[0].strip().replace(".", "").strip()
        # front_image_url = response.meta.get("front_image_url", "")      #文章封面图
        # praise_nums = response.css(".vote-post-up h10::text").extract()[0]
        # # comment_nums = response.css("a[href='#article-commont'] span::text").extract()[0]
        # comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0]
        # fav_nums = response.css(".bookmark-btn::text").extract()[0]
        # match_re = re.match(".*?(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        # content = response.css("div.entry").extract()[0]
        #
        #
        # article_item = JobBoleArticleItem()
        # article_item["url_object_id"] = get_md5(response.url)
        # article_item["title"] = title
        # article_item["url"] = response.url
        # try:
        #     create_date = datetime.datetime.strptime(create_date, "%Y/%m/%d").date()
        # except Exception as e:
        #     create_date = datetime.datetime.now().date()
        # article_item["create_date"] = create_date
        # article_item["front_image_url"] = [front_image_url]
        # article_item["parise_nums"] = praise_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["tags"] = tags
        # article_item["content"] = content
        # #  调用这一句 yield article_item 就会将article_item 传递到pipeline中
        # yield article_item

        front_image_url = response.meta.get("front_image_url", "")
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_css("title", ".entry-header h1::text")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("create_date", "p.entry-meta-hide-on-mobile::text")
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_css("parise_nums", ".vote-post-up h10::text")

        item_loader.add_css("comment_nums", "a[href='#article-comment'] span::text")
        item_loader.add_css("fav_nums", ".bookmark-btn::text")

        item_loader.add_css("tags", "p.entry-meta-hide-on-mobile a::text")
        item_loader.add_css("content", "div.entry")

        article_item = item_loader.load_item()
        yield article_item
        pass
