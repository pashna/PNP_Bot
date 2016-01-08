# -*- coding: utf-8 -*-

__author__ = 'popka'
from TwitterLoader import TwitterLoader
from NewsLoader import NewsLoader
from DataAggregator import DataAggregator
import cPickle
import numpy as np
from datetime import datetime, timedelta

class Engine():

    def __init__(self, first_time=14):
        self.first_time = first_time
        self.twitter_loader = TwitterLoader(first_time)
        self.news_loader = NewsLoader(first_time)
        self.data_aggregator = DataAggregator(first_time)

        self.date = datetime.today() - timedelta(minutes=1)

        with open('gb_regressor.pkl', 'rb') as fid:
            self.model = cPickle.load(fid)

        self.df_features = ["url", "news_date", "week_day_news", "minutes_since_midnight", "first_time_tweet", "follower_sum", "retweeted_count_sum", "user_listed_count_sum", 'VC', 'forbes.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'navalny.com', 'ria.ru', 'roem.ru', 'slon.ru', 'vedomosti.ru', 'vesti.ru']
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

        return df[self.x_features].as_matrix(), df["url"], df["news_date"]


    def predict(self):

        empty = [("", 0, "")]

        news_df = self.news_loader.get_actual_news(self.date)
        tweets_df = self.twitter_loader.get_actual_tweets(self.date)

        if news_df is None or tweets_df is None or len(news_df) == 0 or len(tweets_df) == 0:
            return empty

        df = self.data_aggregator.aggregate_first_time(news_df, tweets_df, self.df_features)

        if len(df) == 0:
            return empty

        X, urls, news_date = self._get_params(df)

        if len(X) == 0:
            return empty

        # Predict
        y = self.model.predict(X)
        y = np.exp(y)-1

        return zip(urls, y, news_date)


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
