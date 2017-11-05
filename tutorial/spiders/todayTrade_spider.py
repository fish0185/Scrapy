import scrapy
import json
import pika
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from bs4 import BeautifulSoup

class QuotesSpider(scrapy.Spider):
    name = "todaytrade"
    queueName = "today_market"
    messageType = "urn:message:Consumer.App.TodayMarket.Model:MarketPost"
    start_urls = [
        'http://www.sydneytoday.com/flea_market'
    ]

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queueName, durable=True)

    def parse(self, response):
        self.logger.info('Parse Pagination ====================> %s', 1)
        print('Parse Pagination ====================> %s', 1)

        #parse landing page
        urls = response.css('.yp-list-item .col-xs-3 a').xpath('@href').extract()
        for url in urls:
            yield response.follow(url, callback=self.parseFullDetailPage, errback=self.errback_httpbin)

        #start parse pagination result
        #yield response.follow('http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p2', callback=self.parseJson, errback=self.errback_httpbin)


    def parseJson(self, response):
        currentPagination = int(response.url[52:])
        self.logger.info('Parse Pagination ====================> %s', currentPagination)
        print('Parse Pagination ====================> %s', currentPagination)
        jsonresponse = json.loads(response.body_as_unicode())
        if jsonresponse['data'] is not None:
            for row in jsonresponse['data']['rows']:
                detailPageUrl = 'http://www.sydneytoday.com/flea_market/' + row['_id']
                hardCode = 'http://www.sydneytoday.com/flea_market/181744240375004'
                yield response.follow(detailPageUrl, callback=self.parseDetailPage, meta=row, errback=self.errback_httpbin)

            pageCount = currentPagination + 1
            #if pageCount < 10:
            nextPageUrl = 'http://www.sydneytoday.com/flea_market-cg0-dl0-bs0-p' + str(pageCount)
            yield response.follow(nextPageUrl, callback=self.parseJson)

    def parseFullDetailPage(self, response):
        productContent = response.css('div.yp-content .col-xs-6')[0]
        contactContent = response.css('div.yp-content .col-xs-6')[1]
        publishedDate = response.css('.yp-toolbar__item::text').extract_first()[5:]
        viewCount = response.css('.yp-toolbar__item::text')[1].extract()[4:-1]
        suburb = response.css('.breadcrumb li')[2].css('::text').extract_first()
        price = productContent.css('.yp-detail')[3].css('::text')[1].extract()
        buyOrSell = productContent.css('.yp-detail')[1].css('::text')[1].extract()
        deliverys = productContent.css('.yp-detail')[2].css('::text')[1].extract()
        contact = response.css('.yp-contact-name::text').extract_first()[4:]
        suburbCode = response.css('.breadcrumb li')[2].xpath('./a/@href').extract_first('')[47:]
        mobileUrl = contactContent.xpath('.//img[contains(@src, "yp-contact-mobile.png")]/following-sibling::img[1]/@src').extract_first()
        cellPhoneUrl = contactContent.xpath('.//img[contains(@src, "yp-contact-cellphone.png")]/following-sibling::img[1]/@src').extract_first()
        phothos = response.css('.yp-gallery img').xpath('@src').extract()
        qq = contactContent.xpath('.//img[contains(@src, "yp-contact-qq.png")]/following-sibling::a[1]/text()').extract_first()
        postFromAus = len(response.css('.alert.alert-warning')) == 0
        htmlDesc = response.css('div.yp-descriprion').extract_first(default='')
        soup = BeautifulSoup(htmlDesc)

        body = {
            'source': 0,
            'title': response.css('div.yp-content h1::text').extract_first(),
            'price': price,
            'mobile': None,
            'telephone': cellPhoneUrl,
            'contact': contact,
            'uid': None,
            'pageUrl': response.url,
            'pageId': response.url[39:],
            'changed': None,
            'photos': phothos,
            'buyOrSell': buyOrSell,
            'buyOrSellType': None,
            'category': None,
            'delivery': None,
            'deliverys': deliverys,
            'publishedTime': None, #发布到用户看到的时间
            'publishedDate': publishedDate,
            'status': None,
            'commNums': None,
            'coverImage': (phothos[:1] or [None])[0],
            'pageViewCount': viewCount,
            'suburb': suburb,
            'suburbCode': suburbCode,
            'categoryText': productContent.xpath('.//div/span/text()')[1].extract(),
            'description': soup.get_text().strip(),
            'emailUrl': contactContent.xpath('.//img[contains(@src, "yp-contact-email.png")]/following-sibling::img[1]/@src').extract_first(),
            'mobileUlr': mobileUrl,
            'qq': qq,
            'wechat': contactContent.xpath('.//img[contains(@src, "yp-contact-wechat.png")]/../text()').extract_first(),
            'fromAus': postFromAus
        }

        masstransitMsg = {
            "messageType": [self.messageType],
            "message": body
        }

        jsonBody = json.dumps(masstransitMsg, ensure_ascii=False).encode('utf8')
        self.channel.basic_publish(exchange='', routing_key=self.queueName, body=jsonBody, properties=pika.BasicProperties(
                         delivery_mode=2
                      ))
        yield body

    def parseDetailPage(self, response):
        productContent = response.css('div.yp-content .col-xs-6')[0]
        contactContent = response.css('div.yp-content .col-xs-6')[1]
        publishedDate = response.css('.yp-toolbar__item::text').extract_first()[5:]
        viewCount = response.css('.yp-toolbar__item::text')[1].extract()[4:-1]
        suburb = response.css('.breadcrumb li')[2].css('::text').extract_first()
        mobileUrl = contactContent.xpath('.//img[contains(@src, "yp-contact-mobile.png")]/following-sibling::img[1]/@src').extract_first()
        qq = contactContent.xpath('.//img[contains(@src, "yp-contact-qq.png")]/following-sibling::a[1]/text()').extract_first()
        postFromAus = len(response.css('.alert.alert-warning')) == 0
        htmlDesc = response.css('div.yp-descriprion').extract_first(default='')
        soup = BeautifulSoup(htmlDesc)
        phothos = response.css('.yp-gallery img').xpath('@src').extract()

        body = {
            'source': 1,
            'title': response.css('div.yp-content h1::text').extract_first(),
            'price': response.meta['jiage'],
            'mobile': response.meta['mobile'],
            'telephone': response.meta['telephone'],
            'contact': response.meta['contact'],
            'uid': response.meta['uid'],
            'pageUrl': response.url,
            'pageId': response.meta['_id'],
            'changed': response.meta['changed'],
            'photos': phothos,
            'buyOrSell': response.meta['buysells'],
            'buyOrSellType': response.meta['buysell'],
            'category': response.meta['category'],
            'delivery': response.meta['delivery'],
            'deliverys': response.meta['deliverys'],
            'publishedTime': response.meta['pub_time'],
            'publishedDate': publishedDate,
            'status': response.meta['status'],
            'commNums': response.meta['comm_nums'],
            'coverImage': (phothos[:1] or [None])[0],
            'pageViewCount': viewCount,
            'suburb': suburb,
            'suburbCode': response.meta['global_placa'],
            'categoryText': productContent.xpath('.//div/span/text()')[1].extract(),
            'description': soup.get_text().strip(),
            'emailUrl': contactContent.xpath('.//img[contains(@src, "yp-contact-email.png")]/following-sibling::img[1]/@src').extract_first(),
            'mobileUlr': mobileUrl,
            'qq': qq,
            'wechat': contactContent.xpath('.//img[contains(@src, "yp-contact-wechat.png")]/../text()').extract_first(),
            'fromAus': postFromAus
        }

        masstransitMsg = {
            "messageType": [self.messageType],
            "message": body
        }

        jsonBody = json.dumps(masstransitMsg, ensure_ascii=False).encode('utf8')
        self.channel.basic_publish(exchange='', routing_key=self.queueName, body=jsonBody, properties=pika.BasicProperties(
                         delivery_mode=2
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


