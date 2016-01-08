# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataProcessors.DataAggregator import DataAggregator
from DataProcessors.FileReader import FileReader
import Config

class Statistic():

    def __init__(self, first_time, last_time, db):
        self.data_aggregator = DataAggregator(first_time, last_time)
        self.df_features = ["url", "last_time_tweet", "last_time_retweet", "news_date"]
        self.loader = FileReader()
        self.db = db


    def get_stats(self, hours):
        """
        Функция возвращает статистику за последние hours часов
        :param hours:
        """
        pass


    def get_df_real(self, hours):
        """
        Функция возвращает аггрегированный DF за hours часов.
        :param hours:
        """
        news_df = self.loader.get_concat_files_by_hours(Config.GET_NEWS_FOLDER(), hours, "url")
        tweets_df = self.loader.get_concat_files_by_hours(Config.GET_TWEETS_FOLDER(), hours, "tw_id")

        df = self.data_aggregator.aggregate_last_time(news_df, tweets_df)

        if df is None:
            return None

        df = df[self.df_features]
        return df.drop_duplicates()


    def get_df_predicted(self):
        pass