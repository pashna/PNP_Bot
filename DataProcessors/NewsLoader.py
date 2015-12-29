# -*- coding: utf-8 -*-
__author__ = 'popka'

from datetime import datetime
from FileReader import FileReader
import numpy as np

class NewsLoader():

    def __init__(self):
        self.loader = FileReader()
        self.last_file_name = "news_2012_12_12_12_12.csv"
        self.OUT_NEWS_FOLDER = "/home/popka/rubbles/StreamingData/test_data_news"



    def get_actual_news(self, time):
        """
        Возвращает новости, которым исполнилось 15 минут
        ПЛЮС-МИНУС МИНУТА????????????????????????????????
        :param time:
        """

        # Обновляем записи
        self._load_new_news()

        #date = datetime.today()#.strftime("%Y-%m-%d %H:%M")
        date = datetime.strptime("2015-12-24 21:55", "%Y-%m-%d %H:%M")#.date()

        def get_minutes_since_date(date, date_from):
            #TODO: Внесто преобразования к типу date, нужно считать точное время, от которого прошло 15 минут и выделять именно такие даты. Проще и быстрее
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')

            #print "{} - {} = {}".format(date_from, date, int((date_from-date).total_seconds()/60))
            return int((date_from-date).total_seconds()/60)

        # Считаем, сколько времени прошло с момента публикации
        self.news_df["time_since_published"] = self.news_df.news_date.apply(lambda s: get_minutes_since_date(s, date))


        # Удаляем устарвшие записи:
        self.news_df = self.news_df[self.news_df["time_since_published"] < time+5]
        print "NEWS_DF = ", len(self.news_df[self.news_df["time_since_published"] == time])
        return self.news_df[self.news_df["time_since_published"] == time]


    def _load_new_news(self):
        """
        Функция считывает новые новости из файла
        """

        df, self.last_file_name = self.loader.get_concated_files(self.OUT_NEWS_FOLDER, "url", self.last_file_name)
        if len(df) == 0:
            return

        if hasattr(self, 'news_df'):
            self.news_df = self.news_df.append(df)
        else:
            self.news_df = df

        dupl = self.news_df["url"].duplicated()
        dupl = np.invert((dupl.as_matrix()))
        self.news_df = self.news_df[dupl]