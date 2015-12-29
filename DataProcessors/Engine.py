# -*- coding: utf-8 -*-

__author__ = 'popka'
from TwitterLoader import TwitterLoader
from NewsLoader import NewsLoader
from DataAggregator import DataAggregator
import cPickle
import numpy as np
import os

class Engine():

    def __init__(self, first_time=14, last_time=200):
        self.first_time = first_time
        self.last_time = last_time
        self.twitter_loader = TwitterLoader()
        self.news_loader = NewsLoader()
        self.data_aggregator = DataAggregator(first_time, last_time)

        with open('gb_regressor.pkl', 'rb') as fid:
            self.model = cPickle.load(fid)

        self.df_features = ["url", "news_date", "week_day_news", "minutes_since_midnight", "first_time_tweet","follower_sum", "retweeted_count_sum", "user_listed_count_sum", 'VC', 'forbes.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'navalny.com', 'ria.ru', 'roem.ru', 'slon.ru', 'vedomosti.ru', 'vesti.ru']
        self.x_features = ["week_day_news", "minutes_since_midnight", "first_time_tweet","follower_sum", "retweeted_count_sum", "user_listed_count_sum",  'VC', 'forbes.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'navalny.com', 'ria.ru', 'roem.ru', 'slon.ru', 'vedomosti.ru', 'vesti.ru']


    def _get_params(self, df):

        df = df[self.df_features]
        print "1   ", len(df)
        print df
        df = df.drop_duplicates().dropna()
        print "2   ", len(df)
        df.reset_index(inplace=True, drop=True)
        print "3   ", len(df)

        return df[self.x_features].as_matrix(), df["url"]


    def predict(self):

        news_df = self.news_loader.get_actual_news(self.first_time)
        tweets_df = self.twitter_loader.get_actual_tweets(self.first_time)


        df = self.data_aggregator.aggregate(news_df, tweets_df, self.df_features)

        X, urls = self._get_params(df)

        if len(X) == 0:
            return ("no_url", 0)

        # Predict
        y = self.model.predict(X)
        y = np.exp(y)-1




        return zip(urls, y)