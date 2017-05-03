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

#  将日期转换为时间戳
def date2stamp(date):

    stamp = date
    return stamp

# 将爬取下的时间换成date的格式
def fix_date(date_in):
    # 今天
    if date_in[0] == u'\u4eca\u5929':
        date_string = time.strftime("%Y%m%d")
    else:
        date_0 = re.findall('\d+',date_in[0])
        date_string = time.strftime("%Y")+str(date_0[0])+str(date_0[1])
    date_1 = re.findall('\d+',date_in[1])
    date_string += str(date_1[0])+str(date_1[1])
    return date_string


class CrawlerWap:
    def __init__(self, crawlday):
        self.deadline = get_timestamp() - crawlday*24*60*60*1000
        self.url = "http://weibo.cn/search/mblog?hideSearchFrame=&keyword="
        self.cook = {"cookie":"**"}
    # weibo_id, weibo_class(1.原创；2.转发),weibo, source(1.空, 2.原微博)，source_id(1.空, 2.原微博id)
    # user_url, user_id, nick_name
    # praise, relay, comment, time
    def crawlbykey(self, keyword, page):
        keyword_new = urllib.quote(keyword)
        url = self.url+keyword_new+'&page='+str(page)
        html = requests.get(url, cookies=self.cook)
        selector = etree.HTML(html.content)

        content = selector.xpath('//div[@class="c"][@id]')
        for each in content:
            weibo_id = each.xpath('@id')[0]
            inform = each.xpath('div/a/text()')
            nick_name = inform[0]
            praise = re.findall('\d+', inform[-4])[0]
            relay = re.findall('\d+', inform[-3])[0]
            comment = re.findall('\d+', inform[-2])[0]
            user_url = each.xpath('div/a[@class="nk"]/@href')[0]
            user_id = user_url.replace('http://weibo.cn/', '')
            pattern = 'u/'
            if pattern in user_id:
                user_id = re.findall('\d+', user_id)[0]

            time_record = each.xpath('div/span[@class="ct"]/text()')[0].split(" ")
            time_pre = time_record[0:2]
            time = fix_date(time_pre)
            # time = date2stamp(time_pre)
            # 查看微博是否为转发
            relay_record = each.xpath('div/span[@class="cmt"]')
            if len(relay_record) > 0:
                weibo_class = 2
                source = each.xpath('div/span[@class="ctt"]')[0].xpath('string(.)')
                weibo = each.xpath('div')[1].xpath('string(.)')
                source_url = inform[-5].replace("https://weibo.cn/comment/","")
                source_id = 'M_'+source_url.split("?")[0]
            else:
                weibo_class = 1
                weibo = each.xpath('div/span[@class="ctt"]')[0].xpath('string(.)')
                source = ""
                source_id = ""

    def crawcomment(self,url_in):
        print 1

    def getcookies(self):
        print 1



c = CrawlerWap(10)
c.crawlbykey("招商银行",10)
