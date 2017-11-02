import scrapy
import json
import pika
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = [
        'http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p2'
    ]

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=32781))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='today', durable=True)

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        if jsonresponse['data'] is not None:
            for row in jsonresponse['data']['rows']:
                detailPageUrl = 'http://www.sydneytoday.com/flea_market/' + row['_id']
                hardCode = 'http://www.sydneytoday.com/flea_market/181744240375004'
                yield response.follow(detailPageUrl, callback=self.parseDetailPage, meta=row, errback = self.errback_httpbin)

            pageCount = int(response.url[52:]) + 1
            self.logger.info('Parse page ====================> %s', pageCount)
            if pageCount < 10:
                nextPageUrl = 'http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p' + str(pageCount)
                yield response.follow(nextPageUrl, callback=self.parse)

    def parseDetailPage(self, response):
        productContent = response.css('div.yp-content .col-xs-6')[0]
        #category = productContent.xpath('.//div/span/text()')[1].extract()
        pri = productContent.css('.yp-detail::text')
        contactContent = response.css('div.yp-content .col-xs-6')[1]
        #chat = contactContent.xpath('.//img[contains(@src, "yp-contact-wechat.png")]/../text()').extract_first()
        #email = contactContent.xpath('.//img[contains(@src, "yp-contact-email.png")]/following-sibling::img[1]/@src').extract_first()
        #if contactContent.xpath('.//img[contains(@src, "yp-contact-wechat.png")]/@src').extract_first() is not None:

        body = {
            'title': response.css('div.yp-content h1::text').extract_first(),
            'price': response.meta['jiage'],
            'mobile': response.meta['mobile'],
            'contact': response.meta['contact'],
            'uid': response.meta['uid'],
            'pageUrl': response.url,
            'pageId': response.meta['_id'],
            'changed': response.meta['changed'],
            'buyOrSell': response.meta['buysells'],
            'deliverys': response.meta['deliverys'],
            'pubTime': response.meta['pub_time'],
            'coverImage': response.meta['cover'],
            'categoryText': productContent.xpath('.//div/span/text()')[1].extract(),
            'description': response.css('div.yp-descriprion::text').extract_first(),
            'emailUrl': contactContent.xpath('.//img[contains(@src, "yp-contact-email.png")]/following-sibling::img[1]/@src').extract_first(),
            'wechat': contactContent.xpath('.//img[contains(@src, "yp-contact-wechat.png")]/../text()').extract_first()
        }

        jsonBody = json.dumps(body,ensure_ascii=False).encode('utf8')
        self.channel.basic_publish(exchange='', routing_key='today', body=jsonBody, properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
        yield body

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)


