# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataBase.DB import DB
import time
from BotEngine.Bot import Bot
from BotEngine.Chats import Chats
from urlparse import urlparse
from datetime import datetime, timedelta


def get_news_type(news):
    url = urlparse(news)
    news = url.netloc
    news = news.replace("www.", "")
    return news


if __name__ == '__main__':

    while 1:
        try:
            db = DB()
            chats = Chats(db)
            bot = Bot(db, chats)
            time.sleep(120)

            while(1):

                date = datetime.today() + timedelta(minutes=1)

                new_news = db.get_new_news()
                for news, predicted in new_news:

                    news_type = get_news_type(news)
                    chats_list = chats.get_relevant_chats(news_type, predicted)
                    bot.send_message(chats_list, news, predicted)

                    db.mark_news_as_sent(news)

                time_to_sleep = int ((date - datetime.today()).total_seconds())

                if (time_to_sleep > 0):
                    time.sleep(time_to_sleep)

        except Exception as e:
            print e
