import time
import threading
import urllib2

from log import Logger
from exception import NoUrlToCrawlError, UrlQueueEmptyForNowError,\
    PageQueueFullForNowError


class Crawler(threading.Thread):
    def __init__(self, cookie, manager):
        threading.Thread.__init__(self)
        cookie_handler = urllib2.HTTPCookieProcessor(cookie)
        self._opener = urllib2.build_opener(cookie_handler)
        self._manager = manager
        self._logger = Logger(u"crawler.log")

    def run(self):
        while True:
            try:
                # fetch url
                url = self._manager.get_url()
                self._logger.debug("Fetch a url " + url)
                # fetch page
                request = urllib2.Request(url)
                page = self._opener.open(request)
                self._logger.debug("Fetch the page for url " + url)
                # insert page
                while True:
                    try:
                        self._manager.insert_page(url, page)
                        self._logger.debug("Insert the page")
                        break
                    except PageQueueFullForNowError:
                        time.sleep(0.1)
            except urllib2.URLError:
                self._manager.insert_page(url, None)
                self._logger.error("URLError for " + url)
            except UrlQueueEmptyForNowError:
                time.sleep(0.1)
            except NoUrlToCrawlError:
                break
