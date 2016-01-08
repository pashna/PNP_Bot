from urlparse import urlparse

__author__ = 'popka'


def get_news_type(news):
    url = urlparse(news)
    news = url.netloc
    news = news.replace("www.", "")
    return news