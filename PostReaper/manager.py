import sys
import time
import threading
import urllib
import urllib2
import cookielib
import ConfigParser
from Queue import Queue, Empty, Full

from bs4 import BeautifulSoup

from crawler import Crawler
from exception import NoUrlToCrawlError, NoPageToParseError,\
    UrlQueueEmptyForNowError, UrlQueueFullForNowError,\
    PageQueueEmptyForNowError, PageQueueFullForNowError


class PostReaper:
    def __init__(self, config_file):
        # init queues
        self._pages_to_parse = Queue(maxsize=100)
        self._pages_parsing = Queue(maxsize=100)

        self._urls_to_crawl = Queue(maxsize=100)
        self._urls_crawling = set()
        self._urls_crawled = set()

        self._lock = threading.Lock()

        # read config file
        self._read_config(config_file)

    def _read_config(self, config_file):
        config_parser = ConfigParser.ConfigParser()
        config_parser.read(config_file)
        self._config = {}
        for section in config_parser.sections():
            for option in config_parser.options(section):
                self._config[option] = config_parser.get(section, option)
        # special handling for some config options
        self._config[u"keywords"] = self._config[u"keywords"].split(",")
        self._config[u"crawler_cnt"] = int(self._config[u"crawler_cnt"])

    def _get_page(self):
        try:
            self._lock.acquire()
            page = self._pages_to_parse.get(block=False)
            self._pages_parsing.put(page)
            return page
        except Empty:
            if len(self._urls_crawling) > 0 or\
                    not self._urls_to_crawl.empty():
                raise PageQueueEmptyForNowError
            else:
                raise NoPageToParseError
        finally:
            self._lock.release()

    def _handle_page(self, page):
        data, urls = self._parse_page(page)
        self._pages_parsing.get()
        self._handle_urls(urls)
        self._handle_data(data)

    def _parse_page(self, page):
        # override in subclass
        # return data, urls
        return None, []

    def _handle_data(self, data):
        # override in subclass
        pass

    def _handle_urls(self, urls):
        while urls:
            try:
                url = urls[-1]
                self._lock.acquire()
                self._urls_to_crawl.put(url)
                urls.pop()
            except UrlQueueFullForNowError:
                time.sleep(0.1)
            finally:
                self._lock.release()

    def _round_off(self):
        # override in subclass
        pass

    def _pre_login(self):
        # override in subclass if neccessary
        pass

    def _login(self):
        # override in subclass if neccessary
        pass

    def get_url(self):
        try:
            self._lock.acquire()
            while True:
                url = self._urls_to_crawl.get(block=False)
                if url not in self._urls_crawling and\
                        url not in self._urls_crawled:
                    self._urls_crawling.add(url)
                    return url
        except Empty:
            if not self._pages_parsing.empty() or\
                    not self._pages_to_parse.empty() or\
                    len(self._urls_crawling) > 0:
                raise UrlQueueEmptyForNowError
            else:
                raise NoUrlToCrawlError
        finally:
            self._lock.release()

    def insert_page(self, url, page):
        try:
            self._lock.acquire()
            self._pages_to_parse.put(page, block=False)
            self._urls_crawling.discard(url)
            self._urls_crawled.add(url)
        except Full:
            raise PageQueueFullForNowError
        finally:
            self._lock.release()

    def start(self):
        # login
        self._pre_login()
        self._login()

        # start crawlers
        self._urls_to_crawl.put(self._config[u"start_url"])
        self._crawlers = [Crawler(self._cookie, self) for _ in
                          range(self._config[u"crawler_cnt"])]
        for crawler in self._crawlers:
            crawler.start()

        # handle pages fetched by crawlers
        while True:
            try:
                page = self._get_page()
                self._handle_page(page)
            except PageQueueEmptyForNowError:
                time.sleep(0.1)
            except NoPageToParseError:
                print "No page to parse."
                break

        # wait crawlers to stop
        print u"Waiting crawlers to stop..."
        for crawler in self._crawlers:
            crawler.join()

        # round-off stay
        self._round_off()

        # all jobs done
        print u"All jobs done."
        print self._successful_page_cnt, u"page(s) fetched."
        print self._failed_page_cnt, u"page(s) failed."
        print self._reaped_post_cnt, u"post(s) reaped."


class UESTCPostReaper(PostReaper):
    def __init__(self, config_file):
        PostReaper.__init__(self, config_file)
        self._successful_page_cnt = 0
        self._failed_page_cnt = 0
        self._reaped_post_cnt = 0
        self._data = []

    # override
    def _pre_login(self):
        login_page = urllib.urlopen(self._config[u"login_url"]).read()
        login_soup = BeautifulSoup(login_page)
        formhash_tag = login_soup.find(u"input", attrs={u"name": u"formhash"})
        self._config[u"formhash"] = formhash_tag[u"value"]

    # override
    def _login(self):
        headers = {u"User-Agent": self._config[u"user-agent"]}
        post_data = {u"answer": self._config[u"answer"],
                     u"formhash": self._config[u"formhash"],
                     u"loginfield": self._config[u"loginfield"],
                     u"loginsubmit": self._config[u"loginsubmit"],
                     u"password": self._config[u"password"],
                     u"questionid": self._config[u"questionid"],
                     u"refer": self._config[u"refer"],
                     u"username": self._config[u"username"]}
        post_data_encoded = urllib.urlencode(post_data)

        self._cookie = cookielib.CookieJar()
        cookie_handler = urllib2.HTTPCookieProcessor(self._cookie)
        self._opener = urllib2.build_opener(cookie_handler)
        request = urllib2.Request(url=self._config[u"login_url"],
                                  data=post_data_encoded,
                                  headers=headers)
        self._opener.open(request)

    # override
    def _parse_page(self, page):
        data = []
        urls = []

        if not page:
            self._failed_page_cnt += 1
        else:
            self._successful_page_cnt += 1

            page_soup = BeautifulSoup(page)
            post_tags = [tag for tag in page_soup.find_all(u"tbody") if
                         (u"id" in tag.attrs and
                             u"normalthread" in tag.attrs[u"id"])]

            for post_tag in post_tags:
                a_tag = post_tag.find(u"a", {u"class": u"s xst"})
                by_tag = post_tag.find(u"td", {u"class": u"by"})
                id_ = post_tag[u"id"].encode(u"utf-8")
                title = a_tag.string.encode(u"utf-8")
                url = a_tag[u"href"].encode(u"utf-8")
                if by_tag:
                    if not by_tag.em.span.span:
                        time = by_tag.em.span.string.encode(u"utf-8")
                    else:
                        time = by_tag.em.span.span[u"title"].encode(u"utf-8")
                else:
                    time = u"unkown"

                if any([keyword in title for keyword in
                        self._config[u"keywords"]]):
                    data.append((id_, title, url, time))
                    self._reaped_post_cnt += 1
                    print self._reaped_post_cnt, u"post(s) reaped."
            pg_div = page_soup.find(u"div", {u"class": "pg"})

            for tag in pg_div.find_all(u"a"):
                url = tag[u"href"]
                if url not in self._urls_crawling and\
                        url not in self._urls_crawled:
                    urls.append(url)

        return data, urls

    # override
    def _handle_data(self, data):
        if data:
            self._data.extend(data)

    # override
    def _round_off(self):
        with open(u"ret", u"w") as fd:
            for data in self._data:
                print type(data[0])
                print type(data[1])
                print type(data[2])
                print type(data[3])
                line = " ".join([data[0], data[1], data[2], data[3], "\n"])
                fd.write(line)


if __name__ == "__main__":
    mgr = UESTCPostReaper(sys.argv[1])
    mgr.start()
