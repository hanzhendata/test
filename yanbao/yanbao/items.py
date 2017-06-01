# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class YanbaoItem(scrapy.Item):
    # define the fields for your item here like:
    
    title   = scrapy.Field()
    stockname    = scrapy.Field()
    stockcode    = scrapy.Field()
    sharedate = scrapy.Field()
    category  = scrapy.Field()
    source = scrapy.Field()
    abstract = scrapy.Field()
    content = scrapy.Field()

    
