# -*- coding: utf-8 -*-

__author__ = 'popka'
from utils.utils import get_news_type
from Config import SERVICED_DOMAINS

class PredictedHandler():

    def __init__(self, db):
        self.db = db


    def handle_predicted(self, predicted):
        """
        функция проверяет, данные, пришедшие от модуля предсказания и, если они ок, то записывает их в базу
        :param predicted: массив кортежей[ (str url, float predicted, str date, int firsttime_tweets)
        """
        for pred in predicted:
            url = pred[0]
            value = float("{0:.2f}".format(pred[1]))
            news_date = pred[2]
            firsttime_tweets = pred[3]

            # Если новость есть, значение неотрицательно и домен входит в число "обслуживаемых"
            if url != "" and \
                value > 0 and \
                (get_news_type(url) in SERVICED_DOMAINS):

                    print (url, value, news_date, firsttime_tweets)
                    self.db.insert_news(url, value, news_date, firsttime_tweets)