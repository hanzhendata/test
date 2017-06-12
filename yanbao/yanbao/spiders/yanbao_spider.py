# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class YanbaoSpider(scrapy.Spider):
	name = "yanbao"
	allowed_domains = ["hibor.com.cn"]
	start_urls = [
		# 'http://hibor.com.cn/docdetail_2095814.html',
		'http://hibor.com.cn/microns_4.html',
	]

	def parse(self, response):
		leftn2 = response.css("div.leftn2")[0].css("table.tab_ltnew").xpath("//span[@class='tab_lta']//a//@href") 
		for url in leftn2.extract():
			url = response.urljoin(url)
			yield scrapy.Request(url, callback=self.parse2)

	def parse2(self, response):
		pmain = response.css("div.p_main").extract_first()
		pass
