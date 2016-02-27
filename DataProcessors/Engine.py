# -*- coding: utf-8 -*-

__author__ = 'popka'
from TwitterLoader import TwitterLoader
from NewsLoader import NewsLoader
from DataAggregator import DataAggregator
import cPickle
import numpy as np
from datetime import datetime, timedelta
import Config
import logging

class Engine():

    def __init__(self, first_time, last_time):
        self.first_time = first_time
        self.last_time = last_time
        self.twitter_loader = TwitterLoader(first_time)
        self.news_loader = NewsLoader(first_time)
        self.data_aggregator = DataAggregator(first_time, last_time)

        self.date = datetime.today() - timedelta(minutes=1)
        #self.date = datetime.strptime("2016-02-11 23:25:29", "%Y-%m-%d %H:%M:%S")

        with open(Config.GET_REGRESSOR(), 'rb') as fid:
            self.model = cPickle.load(fid)

        self.df_features = \
        [

            # DF_ONLY
            'url', 'news_date', 'type', "title",
            #Features
            '15_time_tweet', '15_follower_sum', '12_time_tweet', '12_follower_sum',
            '9_time_tweet', '9_follower_sum', '6_time_tweet', '6_follower_sum',
            '3_time_tweet', '3_follower_sum',
            'is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday',
            'is_day', 'is_morning', 'is_evening', 'is_night',
            'first_time_tweet', 'follower_sum', 'retweeted_count_sum',
            'user_listed_count_sum', 'user_verified_sum', 'user_rank_sum',

            # Y
            'last_time_tweet', 'last_time_retweet',

            # sites
            '1prime.ru', '3dnews.ru', 'TJ_P', 'VC', 'apparat.cc', 'bg.ru', 'cossa.ru', 'ferra.ru',
            'firrma.ru', 'fontanka.ru', 'forbes.ru', 'gazeta.ru', 'geektimes.ru', 'habrahabr.ru', 'inosmi.ru', 'interfax.ru',
            'iphones.ru', 'ixbt.com', 'izvestia.ru', 'kp.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'megamozg.ru', 'msk.kp.ru',
            'navalny.com', 'newsru.com', 'novayagazeta.ru', 'nplus1.ru', 'ntv.ru', 'paperpaper.ru', 'polit.ru', 'radiovesti.ru',
            'rapsinews.ru', 'regnum.ru', 'ria.ru', 'roem.ru', 'russian.rt.com', 'slon.ru', 'svoboda.org', 'tass.ru', 'tvrain.ru', 'vedomosti.ru'

        ]


        self.x_features = \
        [

            #Features
            '15_time_tweet', '15_follower_sum', '12_time_tweet', '12_follower_sum',
            '9_time_tweet', '9_follower_sum', '6_time_tweet', '6_follower_sum',
            '3_time_tweet', '3_follower_sum',
            'is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday',
            'is_day', 'is_morning', 'is_evening', 'is_night',
            'first_time_tweet', 'follower_sum', 'retweeted_count_sum',
            'user_listed_count_sum', 'user_verified_sum', 'user_rank_sum',

            # sites
            '1prime.ru', '3dnews.ru', 'TJ_P', 'VC', 'apparat.cc', 'bg.ru', 'cossa.ru', 'ferra.ru',
            'firrma.ru', 'fontanka.ru', 'forbes.ru', 'gazeta.ru', 'geektimes.ru', 'habrahabr.ru', 'inosmi.ru', 'interfax.ru',
            'iphones.ru', 'ixbt.com', 'izvestia.ru', 'kp.ru', 'lenta.ru', 'lifenews.ru', 'meduza.io', 'megamozg.ru', 'msk.kp.ru',
            'navalny.com', 'newsru.com', 'novayagazeta.ru', 'nplus1.ru', 'ntv.ru', 'paperpaper.ru', 'polit.ru', 'radiovesti.ru',
            'rapsinews.ru', 'regnum.ru', 'ria.ru', 'roem.ru', 'russian.rt.com', 'slon.ru', 'svoboda.org', 'tass.ru', 'tvrain.ru', 'vedomosti.ru'

        ]


    def _get_params(self, df):
        """
        Возвращает необходимые параметры подели
        :param df:
        :return:
        """
        df = df[self.df_features]
        df = df.drop_duplicates().dropna()
        df.reset_index(inplace=True, drop=True)

        return df[self.x_features].as_matrix(), df["url"], df["news_date"], df["first_time_tweet"], df["type"], df["title"]


    def predict(self):

        empty = [("", 0, "", 0, "", "")]

        #debug
        #self.date = datetime.strptime("2016-02-13 03:31:08", "%Y-%m-%d %H:%M:%S")

        news_df = self.news_loader.get_actual_news(self.date)
        tweets_df = self.twitter_loader.get_actual_tweets(self.date)

        if news_df is None or tweets_df is None or len(news_df) == 0 or len(tweets_df) == 0:
            return empty

        df = self.data_aggregator.aggregate_first_time(news_df, tweets_df, self.df_features)

        if len(df) == 0:
            return empty

        X, urls, news_date, first_time_tweets, news_types, titles = self._get_params(df)

        if len(X) == 0:
            return empty

        # Predict
        #print df[["url", "tw_id", "screen_name", "created_at", "news_date", "first_time_tweet"]]

        y = self.model.predict(X)

        return zip(urls, y, news_date, first_time_tweets, news_types, titles)


    def syncClock(self):
        """
        Функция переводит часы на минуту вперед
        возвращает время, которое программа должна спать, чтобы сохранить разницу между реальным временем и программным = 1 минуте
        :return:
        """
        self.date += timedelta(minutes=1)
        sleep_time = int ( ((self.date+timedelta(minutes=1)) - datetime.today()).total_seconds() )

        return sleep_time
