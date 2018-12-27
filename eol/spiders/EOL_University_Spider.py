import pandas as pd
import scrapy
from lxml import etree
from scrapy_splash import SplashRequest


class EOL_University_List_Spider(scrapy.Spider):
    name = "eol_universty_detail"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.university_df = pd.read_csv('university.csv')
        self.university_info = []
        self.url_count = {}

    def get_uid(self, url: str):
        end = url.index('.')
        nums = [str(_) for _ in range(10)]
        start = end - 1
        while (url[start] in nums):
            start -= 1
        return int(url[start + 1:end])

    def extract_uid(self, url: str):
        start = 42
        nums = [str(_) for _ in range(10)]
        end = start + 1
        while (url[end] in nums):
            end += 1
        return url[start:end]

    def start_requests(self):
        self.script = """
            assert(splash:go(args.url))
            assert(splash:wait(10))
            local btn=splash:select('body > div.zhezhao > div > div.citybox.clearfix > div:nth-child(16)')
            btn:mouse_click()
            assert(splash:wait(10))
            return {
                html = splash:html()
            }
        """
        for idx, row in self.university_df.iterrows():
            uid = self.get_uid(row['url'])
            url = 'https://gkcx.eol.cn/schoolhtm/schoolInfo/%d/10056/detail.htm' % uid
            yield SplashRequest(url=url, callback=self.parse,
                                args={'lua_source': self.script}, endpoint='run')

    def parse(self, response):
        htmlparser = etree.HTMLParser()
        tree = etree.fromstring(response.text, htmlparser)
        university_name_node = tree.xpath('//div[@class="li-collegeUl"]/p/span')
        if (university_name_node is None or len(university_name_node) == 0):
            return SplashRequest(url=response.url, callback=self.parse,
                                 args={'lua_source': self.script}, endpoint='run')
        try:
            university_name = university_name_node[0].text
            tags_node = tree.xpath('//div[@class="li-collegeUl"]/p/a')
            tags = []
            for tag in tags_node:
                tags.append(tag.text)

            # level & phone
            li_nodes = tree.xpath('//ul[@class="li-collegeInfo"]/li')
            level = li_nodes[2][0].text
            phone = li_nodes[3][0][0].text

            # communicate
            li_nodes = tree.xpath('//ul[@class="li-collegeInfo li-ellipsis"]/li')
            email = li_nodes[0][0][0].text
            location = li_nodes[1][0][0].text
            website = li_nodes[2][0][0].text

            # introduction
            introduction = ''
            for p in tree.xpath('//div[@class="content news"]/p'):
                if (p.text is not None):
                    introduction += p.text + '\n'
            uid = self.extract_uid(response.url)
            self.university_info.append(
                [uid, university_name, '/'.join(tags), level, phone, email, location, website, introduction])
            print(len(self.university_info))
        except Exception as e:
            if (response.url not in self.url_count):
                self.url_count[response.url] = 0
            self.url_count[response.url] += 1
            if (self.url_count[response.url] <= 3):
                return SplashRequest(url=response.url, callback=self.parse,
                                     args={'lua_source': self.script}, endpoint='run')

    def closed(self, reason):
        df = pd.DataFrame(data=self.university_info,
                          columns=['uid', 'name', 'tags', 'level', 'phone', 'email', 'location', 'website',
                                   'introduction'])
        df.to_csv('university_info.csv')
