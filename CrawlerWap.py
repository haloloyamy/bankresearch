#!usr/bin/python  
# -*- coding:utf-8 -*- 
"""
Created by Haloloyamy on 2017/5/1
"""
import requests
import json
import base64
import re
import lxml.etree as etree
import time
import urllib
import MySQLdb


# 获取当前系统时间戳(毫秒为单位)
def get_timestamp():
    try:
        tamp = time.time()
        timestamp = str(int(tamp))+"000"
        return int(timestamp)
    except Exception, e:
        print e
    finally:
        pass


#  将日期201705010000转换为时间戳
def date2stamp(date):
    date_norm = time.strptime(date, "%Y%m%d%H%M")
    stamp = str(int(time.mktime(date_norm)))+"000"
    return int(stamp)


# 将爬取下的时间换成date的格式
def fix_date(date_in):
    # print date_in
    # 几分钟前 \u5206\u949f\u524d
    sub = u'\u5206\u949f\u524d'
    if len(date_in) == 1 or sub in date_in[0]:
        date_string = time.strftime("%Y%m%d")+'0000'
        return date_string
    elif date_in[0] == u'\u4eca\u5929':
        date_string = time.strftime("%Y%m%d")
    else:
        date_0 = re.findall('\d+', date_in[0])
        date_string = time.strftime("%Y")+str(date_0[0])+str(date_0[1])
    date_1 = re.findall('\d+', date_in[1])
    date_string += str(date_1[0])+str(date_1[1])
    return date_string


class CrawlerWap:
    def __init__(self, crawlday):
        self.deadline = get_timestamp() - crawlday*24*60*60*1000
        url = "http://weibo.cn/search/mblog?hideSearchFrame=&keyword="
        self.cook = {"cookie": "cookie=_T_WM=4d74ae138f1dde025f5514231908306b; SCF=AgFcYO36Y07pPER4UhWS7UfNSt1WRu4radY76dOP1y56QKXhiwd33jSSbwij8P6R7-v83t0gBbV3-0Dy-mEP2G4.; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWEyJSyhdadayw1oDnggcsj5JpX5o2p5NHD95QcehecehBXSoMRWs4Dqcj6i--ciKnRiK.pi--ciK.Ri-8si--NiK.4i-i8i--fiKysiK.Ri--fi-2fi-z0i--4iKnNiK.pi--fiKnRiKLW; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D102803_ctg1_8999_-_ctg1_8999_home; SUB=_2A250DAgPDeRhGeBO6FQR9CrKzDSIHXVXDqhHrDV6PUJbkdBeLRbskW1N0E5Fn-UD72IDEbPGiYC032sSog..; SUHB=0u5REVJJ7drCgh; SSOLoginState=1493727327"}
        self.conn = MySQLdb.connect('127.0.0.1', 'root', '****', 'weibo_crawler', charset='utf8')
        self.cur = self.conn.cursor()
        self.cur.execute('select * from keyword_record')
        results = self.cur.fetchall()
        for keyword in results:
            self.url = url+keyword[1]
            page = self.crawpage(self.url)
            print "开始爬取", keyword[0], "page:", page
            if page > 0:  # page=0则该关键词没有对应微博
                for i in xrange(1, 3):  # page+1
                    print keyword[1], "第", i, "页"
                    weibo_time = self.crawlbykey(i, keyword[0])
                    if weibo_time < self.deadline:
                        break
        self.conn.commit()
        print "关键词微博及评论爬取完毕----"
        print "开始爬取参与用户信息----"
        self.cur.execute('select user_id from weibo_inform')
        users = self.cur.fetchall()
        for u in users:
            try:
                self.crawluser(u[0])
            except Exception, e:
                print e
        self.conn.commit()
        self.cur.execute('select c_user_id from weibo_comment')
        users_c = self.cur.fetchall()
        for u_c in users_c:
            try:
                self.crawluser(u_c[0])
            except Exception, e:
                print e
        self.conn.commit()


    # 爬取包含关键词的微博网页数
    def crawpage(self, url_in):
        html = requests.get(url_in, cookies=self.cook)
        selector = etree.HTML(html.content)
        # print html.content
        pagelist = selector.xpath('//div[@id="pagelist"]/form/div/text()')
        # print pagelist
        if len(pagelist)>0:
            page = re.findall('\d+', pagelist[1])[-1]
        else:
            page = 0
        return page

    # weibo_id, weibo_class(1.原创；2.转发),weibo, source(1.空, 2.原微博)，source_id(1.空, 2.原微博id)
    # user_url, user_id, nick_name
    # praise, relay, comment, time
    def crawlbykey(self, page,key_id):
        # keyword = urllib.quote(keyword)
        url = self.url+'&page='+str(page)
        html = requests.get(url, cookies=self.cook)
        selector = etree.HTML(html.content)

        content = selector.xpath('//div[@class="c"][@id]')
        for each in content:
            weibo_inform_crawl = [key_id]
            weibo_id = each.xpath('@id')[0]
            inform = each.xpath('div/a/text()')
            nick_name = inform[0].encode('utf-8')
            praise = re.findall('\d+', inform[-4])[0]
            relay = re.findall('\d+', inform[-3])[0]

            comment = re.findall('\d+', inform[-2])[0]
            # 当微博评论大于0时，爬取第一页的评论内容
            if int(comment) > 0:
                self.crawlcomment(weibo_id)

            user_url = each.xpath('div/a[@class="nk"]/@href')[0]
            user_id = user_url.replace('https://weibo.cn/', '').replace('http://weibo.cn/','')

            time_record = each.xpath('div/span[@class="ct"]/text()')[0].split(" ")
            time_pre = time_record[0:2]
            launch_time = fix_date(time_pre)

            # 查看微博是否为转发
            relay_record = each.xpath('div/span[@class="cmt"]')
            if len(relay_record) > 0:
                weibo_class = 2
                source = each.xpath('div/span[@class="ctt"]')[0].xpath('string(.)').encode('utf-8')
                weibo = each.xpath('div')[-1].xpath('string(.)').encode('utf-8')
                if len(each.xpath('div'))<3:
                    source_url = each.xpath('div[1]/a/@href')[-1]
                else:
                    source_url = each.xpath('div[2]/a/@href')[-1]
                source_url = source_url.replace("https://weibo.cn/comment/","").replace("http://weibo.cn/comment/","")
                source_id = 'M_'+source_url.split("?")[0]
            else:
                weibo_class = 1
                weibo = each.xpath('div/span[@class="ctt"]')[0].xpath('string(.)').encode('utf-8')
                source = ""
                source_id = ""

            weibo_inform_crawl += [weibo_id,nick_name,praise,relay,comment,user_url,user_id,weibo_class,weibo,source,source_id,launch_time]
            #weibo_inform_crawl += ['M_F1q907EtS',u'1','1','1','1','1','1',1,u'\u8d35\u8d35\u662f\u6211\u6211\u662f\u8d35\u8d35','1','1','1']
            # print weibo_inform_crawl
            self.insert_record(weibo_inform_crawl, 'weibo_inform')
        return date2stamp(launch_time)

    def crawlcomment(self, wb_id):
        comment_url = "https://weibo.cn/comment/" + wb_id.replace('M_', '')
        html_c = requests.get(comment_url, cookies=self.cook)
        selector_c = etree.HTML(html_c.content)
        all_txt = selector_c.xpath('//div[@class="c"][@id]')
        comment = all_txt[1:len(all_txt)]
        for each in comment:
            comment_value = [wb_id]
            c_user_id = each.xpath('a/@href')[0]
            c_user_id = c_user_id[1:len(c_user_id)]
            comment_txt = each.xpath('span[@class="ctt"]')[0].xpath('string(.)').encode('utf-8')
            comment_date = each.xpath('span[@class="ct"]/text()')[0].split(" ")
            comment_time = fix_date(comment_date[0:2])
            comment_value += [c_user_id, comment_txt, comment_time]
            self.insert_record(comment_value, 'weibo_comment')

    def crawluser(self, u_id):
        user_value = [u_id]
        print u_id
        u_url = "http://weibo.cn/"+u_id
        html_u = requests.get(u_url, cookies=self.cook)
        selector_u = etree.HTML(html_u.content)
        pre_information = selector_u.xpath('//div[@class="ut"]')[0]
        sex = 0  # girl
        daren = 0
        vip = 0
        M = 0
        identification = ""
        if len(pre_information.xpath('span[@class="ctt"]')) < 1:
            sex = ' '
            location = u'其他'
            information = pre_information
        else:
            information = pre_information.xpath('span[@class="ctt"]')[0]
            if len(information.xpath('img[@alt]')) == 0 and len(information.xpath('a/img[@alt]')) == 0:
                sex_loc = information.xpath('text()')[0].split('/')
            else:
                sex_loc = information.xpath('text()')[1].split('/')
            if u'男' in sex_loc[0]:
                sex = 1
            location = sex_loc[1].replace(' ', '').encode('utf-8')

        if len(information.xpath('img[@alt]')) > len(information.xpath('img[@alt="V"]')):
            daren = 1
        if len(information.xpath('img[@alt="V"]')) > 0:
            vip = 1
            identification = selector_u.xpath('//span[@class="ctt"]')[1].xpath('text()')[0].encode('utf-8')
        if len(information.xpath('a/img[@alt="M"]')) > 0:
            M = 1

        fans = selector_u.xpath('//div[@class="tip2"]/a/text()')[1]
        f_num = re.findall('\d+', fans)[0]

        user_value += [daren, vip, M, sex, location, identification, f_num]
        self.insert_record(user_value, 'user_involved_inform')

    def insert_record(self, value, table):
        try:
            if table == 'weibo_inform':
                self.cur.execute('insert IGNORE into '+table+'(key_id,weibo_id,nick_name,praise,relay,comment_wb,user_url,user_id,weibo_class,weibo,source,source_id,launch_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',value)
            elif table == 'weibo_comment':
                self.cur.execute('insert IGNORE into '+table+'(weibo_id,c_user_id,comment_txt,comment_time) value(%s,%s,%s,%s)', value)
            else:
                self.cur.execute('insert IGNORE into '+table+'(user_id, daren, vip, M, sex, location, identification, f_num) value(%s,%s,%s,%s,%s,%s,%s,%s)', value)
            self.conn.commit()
        except:
            print "rollback"
            self.conn.rollback()

    def getcookies(self):
        print 1



c = CrawlerWap(10)