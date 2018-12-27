import pandas as pd
import scrapy
from lxml import etree
from scrapy_splash import SplashRequest


class EOL_University_List_Spider(scrapy.Spider):
    name = "eol_universty_province_score"

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.province_score = []
        self.counter = {}
        self.provinces = {10000: "上海", 10001: "云南", 10002: "内蒙古", 10003: "北京", 10004: "吉林", 10005: "四川", 10006: "天津",
                          10007: "宁夏", 10008: "安徽", 10009: "山东", 10010: "山西", 10011: "广东", 10012: "广西", 10013: "新疆",
                          10014: "江苏", 10015: "江西", 10016: "河北", 10017: "河南", 10018: "浙江", 10019: "海南", 10020: "香港",
                          10021: "湖北", 10022: "湖南", 10023: "甘肃", 10024: "福建", 10025: "西藏", 10026: "贵州", 10027: "辽宁",
                          10028: "重庆", 10029: "陕西", 10030: "青海", 10031: "黑龙江"}
        self.subjects = {10034: "文科", 10035: "理科", 10166: "不分科类"}  # 10090: "综合", 10091: "艺术类", 10093: "体育类", }
        self.batches = {10036: '一批', 10037: '二批', 10038: '三批', 10148: '专科', 10149: '本科提前批', 10162: '普通类提前批',
                        10154: '本科批', 10155: '专科批', 10158: '专科提前批'}
        self.university_list = pd.read_csv('university.csv')

    def start_requests(self):
        self.script = """
        local function isNotNULL(obj)
            return obj ~= nil
        end
        
        function main(splash, args)
            assert(splash:go(args.url))
            assert(splash:wait(5))
            return {
                url = splash:url(),
                html = splash:html()
            }
        end
        """
        for idx, row in self.university_list.iterrows():
            for pid, province in self.provinces.items():
                for sid, subject in self.subjects.items():
                    for bid, batch in self.batches.items():
                        url = 'https://gkcx.eol.cn/schoolhtm/schoolAreaPoint/%d/%d/%d/%d.htm' % (
                        row['uid'], pid, sid, bid)

                        yield SplashRequest(url=url, callback=self.parse,
                                            args={'lua_source': self.script}, endpoint='execute')

    def parse(self, response):
        htmlparser = etree.HTMLParser()
        tree = etree.fromstring(response.text, htmlparser)
        university_name_node = tree.xpath('//div[@class="li-collegeUl"]/p/span')
        if ('https://gkcx.eol.cn/404.htm' == response.url):
            return None
        elif (university_name_node is None or len(university_name_node) == 0):
            if (response.url not in self.counter):
                self.counter[response.url] = 0
            self.counter[response.url] += 1
            if (self.counter[response.url] < 4):
                return SplashRequest(url=response.url, callback=self.parse,
                                     args={'lua_source': self.script}, endpoint='execute')
        else:
            try:
                uid, pid, sid, bid = self.parser_url(response.url)
                trs_nodes = tree.xpath('//div[@class="places-tab margin20"]/table/tbody/tr')
                for tr_node in trs_nodes:
                    if (len(tr_node) < 4):
                        break
                    year = tr_node[0].text
                    score_max = tr_node[1].text
                    score_avg = tr_node[2].text
                    score_min = tr_node[3].text
                    score_province_control = tr_node[4].text
                    self.province_score.append(
                        [uid, pid, self.provinces[pid], sid, self.subjects[sid], bid, self.batches[bid], year,
                         score_max, score_avg, score_min, score_province_control])
                print(len(self.province_score))
            except Exception as e:
                if (response.url not in self.counter):
                    self.counter[response.url] = 0
                self.counter[response.url] += 1
                if (self.counter[response.url] < 4):
                    return SplashRequest(url=response.url, callback=self.parse,
                                         args={'lua_source': self.script}, endpoint='execute')

    def parser_url(self, url: str):
        segments = url.split('/')
        return [int(segments[5]), int(segments[6]), int(segments[7]), int(segments[8].split('.')[0])]

    def closed(self, reason):
        df = pd.DataFrame(data=self.province_score,
                          columns=['uid', 'pid', 'province', 'sid', 'subject', 'bid', 'batch', 'year', 'score_max',
                                   'score_avg', 'score_min', 'score_province_control'])
        df.to_csv('university_province_score.csv')
