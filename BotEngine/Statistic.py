# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataProcessors.DataAggregator import DataAggregator
from DataProcessors.FileReader import FileReader
import Config
from datetime import datetime, timedelta
import pandas as pd
from utils.utils import get_news_type
from math import exp
from Config import DEFAULT_THRESHOLD
import logging


class Statistic():

    def __init__(self, first_time, last_time, db):
        self.data_aggregator = DataAggregator(first_time, last_time)
        self.df_features = ["url", Config.GET_PREDICTED_FEATURE(), "news_date"]
        self.loader = FileReader()
        self.db = db



    def get_statistic(self, hours, restriction):
        result_matrix = self._get_df_from_db(hours)

        if len(result_matrix)==0:# is None:
            return None

        result = {"missed":0, "correct":0, "error": 0, "filtered": 0, "all": 0}

        for row in result_matrix:
            type = get_news_type(row[0])
            if type in restriction:
                threshold = restriction[type]
            else:
                threshold = self.get_default_threshold(type)

            real = row[1]
            predicted = row[2]

            # если угадали правильно
            if real > threshold and predicted > threshold:
                result["correct"] += 1

            # если ошибочно решили, что она топовая
            if real < threshold and predicted > threshold:
                result["error"] += 1

            # если пропустили популярную новость
            if real > threshold and predicted < threshold:
                result["missed"] += 1

            # если правильно отсейяли
            if real < threshold and predicted < threshold:
                result["filtered"] += 1


            result["all"] += 1

        return result


    def _get_division(self, y):
        return 0.4/(1+exp(-0.003*y))


    def _get_df_from_db(self, hours):
        """
        Возвращает все новости из таблицы news в формате DF, которые опубликованы в последние hours часов
        :param hours:
        """
        # Вычисляем время, за hours часов до текущего момента
        date = datetime.today() - timedelta(hours=hours)
        date = date.strftime('%Y-%m-%d %H:%M')
        print date
        db_values = list(self.db.get_news_after_date(date))
        print db_values

        return db_values


    def get_default_threshold(self, type):
        """
        Возвращает значение порога, по которому будет расчитана правильность статистики.
        Если по какой-то причине нет новости такого типа, то возвращаем заведомо большое число 555555.
        :param type:
        :return:
        """
        threshold = 555555
        try:
            threshold = DEFAULT_THRESHOLD[type]
        except Exception as e:
            logging.exception("exception")

        return threshold