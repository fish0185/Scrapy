import scrapy
import json

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = [
        'http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p2'
    ]

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        if jsonresponse['data'] is not None:
            for row in jsonresponse['data']['rows']:
                detailPageUrl = 'http://www.sydneytoday.com/flea_market/' + row['_id']
                yield response.follow(detailPageUrl, callback=self.parseDetailPage)

            pageCount = int(response.url[52:]) + 1
            if pageCount < 2:
                nextPageUrl = 'http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p' + str(pageCount)
                yield response.follow(nextPageUrl, callback=self.parse)

    def parseDetailPage(self, response):
        content = response.css('div.yp-content .yp-detail ::text')[0].extract()
        content1 = response.css('div.yp-content .yp-detail ::text')[1].extract()
        content2 = response.css('div.yp-content .yp-detail ::text')[2].extract()
        content3 = response.css('div.yp-content .yp-detail ::text')[3].extract()
        content4 = response.css('div.yp-content .yp-detail ::text')[4].extract()
        content5 = response.css('div.yp-content .yp-detail ::text')[5].extract()
        yield {
            'title': response.css('div.yp-content h1::text').extract_first(),
            'price': response.css('div.yp-content .yp-detail')[3].extract(),
            'description': response.css('div.yp-descriprion::text').extract_first()
        }


