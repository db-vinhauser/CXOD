import pandas as pd
import scrapy
from lxml import etree
from scrapy_splash import SplashRequest


class EOL_University_List_Spider(scrapy.Spider):
    name = "eol_universty_list"

    def start_requests(self):
        self.university_list = []
        self.script = """
            local headers = 
            assert(splash:go(args.url))
            assert(splash:wait(15))
            local btn=splash:select('body > div.zhezhao > div > div.citybox.clearfix > div:nth-child(16)')
            btn:mouse_click()
            assert(splash:wait(1))
            return {
                html = splash:html()
            }
        """
        urls = []
        for i in range(1, 96):
            urls.append('https://gkcx.eol.cn/soudaxue/queryschool.html?&page=%d' % i)
        for url in urls:
            yield SplashRequest(url=url, callback=self.parse,
                                args={'lua_source': self.script}, endpoint='run')

    def parse(self, response):
        htmlparser = etree.HTMLParser()
        tree = etree.fromstring(response.text, htmlparser)
        nodes = tree.xpath('//*[@id="seachtab"]/tbody/tr[@class="lin-gettr"]')
        if (len(nodes) == 0):
            return SplashRequest(url=response.url, callback=self.parse,
                                 args={'lua_source': self.script}, endpoint='run')
        for node in nodes:
            td0 = node[0][0]
            name = td0.text
            url = td0.get('href')
            alias_name = td0.get('title')

            td1 = node[1]
            province = td1.text

            td3 = node[3]
            type = td3.text

            td5 = node[5]
            type_rank = td5.text

            self.university_list.append([name, url, alias_name, province, type, type_rank])
        print(len(self.university_list))

    def closed(self, reason):
        df = pd.DataFrame(data=self.university_list, columns=['name', 'url', 'alias', 'province', 'type', 'type_rank'])
        df.to_csv('university.csv')
