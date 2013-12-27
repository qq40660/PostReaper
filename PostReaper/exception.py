class NoUrlToCrawlError(Exception):
    pass


class NoPageToParseError(Exception):
    pass


class UrlQueueEmptyForNowError(Exception):
    pass


class UrlQueueFullForNowError(Exception):
    pass


class PageQueueEmptyForNowError(Exception):
    pass


class PageQueueFullForNowError(Exception):
    pass
