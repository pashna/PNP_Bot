# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataBase.DB import DB
import time
from BotEngine.Bot import Bot
from BotEngine.Chats import Chats
from urlparse import urlparse


def get_news_type(news):
    url = urlparse(news)
    news = url.netloc
    news = news.replace("www.", "")
    return news


if __name__ == '__main__':

    db = DB()
    chats = Chats(db)
    bot = Bot(db, chats)

    time.sleep(20)

    while(1):

        new_news = db.get_new_news()
        for news, predicted in new_news:

            news_type = get_news_type(news)
            chats_list = chats.get_relevant_chats(news_type, predicted)
            bot.send_message(chats_list, news, predicted)

            db.mark_news_as_sent(news)

        time.sleep(10)