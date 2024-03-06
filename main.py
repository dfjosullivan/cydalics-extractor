from scrapy.crawler import CrawlerProcess

from url_scraper.spiders.url_spider import LinkDownloaderSpider

if __name__ == "__main__":
    spider = LinkDownloaderSpider

    process = CrawlerProcess()
    process.crawl(spider)
    process.start()