# -*- coding: utf-8 -*-
__author__ = 'popka'

from datetime import datetime
from FileReader import FileReader
import numpy as np
from Config import GET_NEWS_FOLDER

class NewsLoader():

    def __init__(self, time):
        self.loader = FileReader()
        self.last_file_name = "news_2012_12_12_12_12.csv"
        self.OUT_NEWS_FOLDER = GET_NEWS_FOLDER()
        self.time = time
        self.news_df = None



    def get_actual_news(self, date):
        """
        Возвращает новости, которым исполнилось self.time минут с момента date
        :param date: время, "в которое" считаем
        """

        # Обновляем записи
        self._load_new_news(date)

        if self.news_df is None:
            return None

        def get_minutes_since_date(date, date_from):
            #TODO: Внесто преобразования к типу date, нужно считать точное время, от которого прошло 15 минут и выделять именно такие даты. Проще и быстрее
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')

            return int((date_from-date).total_seconds()/60)

        # Считаем, сколько времени прошло с момента публикации
        self.news_df["time_since_published"] = self.news_df.news_date.apply(lambda s: get_minutes_since_date(s, date))


        # Удаляем устарвшие записи:
        self.news_df = self.news_df[self.news_df["time_since_published"] < self.time+5]

        return self.news_df[self.news_df["time_since_published"] == self.time]


    def _load_new_news(self, date):
        """
        Функция считывает новые новости из файла,
        Удаляет повторения
        """

        df, self.last_file_name = self.loader.get_concated_files(self.OUT_NEWS_FOLDER, date, "url", self.last_file_name)
        if df is None:
            print "EMPTY"
            return None

        if self.news_df is not None:
            self.news_df = self.news_df.append(df)
        else:
            self.news_df = df

        dupl = self.news_df["url"].duplicated()
        dupl = np.invert((dupl.as_matrix()))
        self.news_df = self.news_df[dupl]
        return 1