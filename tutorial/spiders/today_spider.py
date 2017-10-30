import scrapy

class TodaySpider(scrapy.Spider):
    name = 'today'
    start_urls = ['http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p2']

    def parse(self, response):
        print(response.body)
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').extract_first(),
                'author': quote.css('small.author::text').extract_first(),
                'tags': quote.css('div.tags a.tag::text').extract(),
            }



