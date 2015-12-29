# -*- coding: utf-8 -*-

__author__ = 'popka'
from TwitterLoader import TwitterLoader
from NewsLoader import NewsLoader
from DataAggregator import DataAggregator
import cPickle
import numpy as np
from datetime import datetime, timedelta
import os

class Engine():

    def __init__(self, first_time=14):
        self.first_time = first_time
        self.twitter_loader = TwitterLoader(first_time)
        self.news_loader = NewsLoader(first_time)
        self.data_aggregator = DataAggregator(first_time)

        self.date = datetime.today() - timedelta(minutes=1)
        print(self.date)

        with open('gb_regressor.pkl', 'rb') as fid:
            self.model = cPickle.load(fid)

        self.df_features = ["url", "news_date", "week_day_news", "minutes_since_midnight", "first_time_tweet","follower_sum", "retweeted_count_sum", "user_listed_count_sum", 'VC', 'forbes.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'navalny.com', 'ria.ru', 'roem.ru', 'slon.ru', 'vedomosti.ru', 'vesti.ru']
        self.x_features = ["week_day_news", "minutes_since_midnight", "first_time_tweet","follower_sum", "retweeted_count_sum", "user_listed_count_sum",  'VC', 'forbes.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'navalny.com', 'ria.ru', 'roem.ru', 'slon.ru', 'vedomosti.ru', 'vesti.ru']


    def _get_params(self, df):
        """
        Возвращает необходимые параметры подели
        :param df:
        :return:
        """
        df = df[self.df_features]
        df = df.drop_duplicates().dropna()
        df.reset_index(inplace=True, drop=True)

        return df[self.x_features].as_matrix(), df["url"]


    def predict(self):

        empty = [("no_url", 0)]

        news_df = self.news_loader.get_actual_news(self.date)
        tweets_df = self.twitter_loader.get_actual_tweets(self.date)

        if len(news_df) == 0 or len(tweets_df) == 0:
            print "tweets or news empty"
            print "tweets_length = {},   news_length = {}".format(len(tweets_df), len(news_df))
            return empty

        df = self.data_aggregator.aggregate(news_df, tweets_df, self.df_features)

        if len(df) == 0:
            print "aggregate empty"
            return empty

        X, urls = self._get_params(df)

        if len(X) == 0:
            return empty

        # Predict
        y = self.model.predict(X)
        y = np.exp(y)-1


        return zip(urls, y)


    def syncClock(self):
        """
        Функция переводит часы на минуту вперед
        возвращает время, которое программа должна спать, чтобы сохранить разницу между реальным временем и программным = 1 минуте
        :return:
        """
        self.date += timedelta(minutes=1)
        print "prog_time = {}    real_time = {}".format(self.date, datetime.today())
        sleep_time = int ( ((self.date+timedelta(minutes=1)) - datetime.today()).total_seconds() )

        return sleep_time
