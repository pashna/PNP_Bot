# -*- coding: utf-8 -*-

__author__ = 'popka'

from FileReader import FileReader
from datetime import datetime
import numpy as np
from Config import GET_TWEETS_FOLDER

class TwitterLoader():

    def __init__(self, time):
        self.loader = FileReader()
        self.last_file_name = "tw_2012_12_12_12_12"
        self.OUT_TWEETS_FOLDER = GET_TWEETS_FOLDER()
        self.time = time
        self.tweets_df = None


    def get_actual_tweets(self, date):

        """
        Возвращает новости, которым исполнилось 15 минут
        ПЛЮС-МИНУС МИНУТА????????????????????????????????
        :param time:
        """

        # Обновляем записи
        self.load_new_tweets(date)

        if self.tweets_df is None:
            return None

        def get_minutes_since_date(date, date_from):
            #TODO: Внесто преобразования к типу date, нужно считать точное время, от которого прошло 15 минут и выделять именно такие даты. Проще и быстрее
            date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

            return int((date_from-date).total_seconds()/60)

        # Считаем, сколько времени прошло с момента публикации
        self.tweets_df["time_since_published"] = self.tweets_df.created_at.apply(lambda s: get_minutes_since_date(s, date))

        # Удаляем устарвшие записи:
        self.tweets_df = self.tweets_df[self.tweets_df["time_since_published"] < self.time]

        return self.tweets_df




    def load_new_tweets(self, date):
        """
        Функция считывает новые твитты из файла
        """

        df, self.last_file_name = self.loader.get_concated_files(self.OUT_TWEETS_FOLDER, date, "tw_id", self.last_file_name)
        if df is None:
            return None

        if self.tweets_df is not None:
            self.tweets_df = self.tweets_df.append(df)
        else:
            self.tweets_df = df

        dupl = self.tweets_df["tw_id"].duplicated()
        dupl = np.invert((dupl.as_matrix()))
        self.tweets_df = self.tweets_df[dupl]
        return 1