# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataProcessors.DataAggregator import DataAggregator
from DataProcessors.FileReader import FileReader
import Config
from datetime import datetime, timedelta
import pandas as pd
from utils.utils import get_news_type
from math import exp


class Statistic():

    def __init__(self, first_time, last_time, db):
        self.data_aggregator = DataAggregator(first_time, last_time)
        self.df_features = ["url", Config.GET_PREDICTED_FEATURE(), "news_date"]
        self.loader = FileReader()
        self.db = db



    def get_statistic(self, hours, restriction):
        result_matrix = self._get_real_and_predicted(hours)

        if result_matrix is None:
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


    """
    def get_statistic(self, hours, restriction):

        :param hours: Время, за которое собираем статистику
        :param restriction: ограничения для пользователя, который ее запрашивает
        :param deviation: ошибка в процентах, которую считаем адекватной. Может, заменить функцией какой-нибудь?
        :return:

        result_matrix = self._get_real_and_predicted(hours)

        result = {"missed":0, "correct":0, "error": 0, "all": 0, "ndcg": 0}



        showed_real_predicted = []
        for row in result_matrix:
            type = get_news_type(row[0])

            real = row[1]
            predicted = row[2]

            if type in restriction:
                threshold = restriction[type]
            else:
                threshold = self.get_default_threshold(type)

            print "!", real, predicted, threshold, type
            if real > threshold or predicted > threshold:

                showed_real_predicted.append((real, predicted))

                print real, "-", predicted, '<' , self._get_division(real)*real

                if abs(real - predicted) < self._get_division(real)*real: #округляем в большую сторону
                    result["correct"] += 1

                elif real > predicted:
                    result["missed"] += 1

                elif real < predicted:
                    result["error"] += 1


            if real > threshold:
                result["all"] += 1

        return result

    """

    def _get_division(self, y):
        return 0.4/(1+exp(-0.003*y))



    def _get_real_and_predicted(self, hours):
        real_df = self._get_df_real(hours)
        predicted_df = self._get_df_from_db(hours)

        if (real_df is None) or (predicted_df is None):
            return None

        df = predicted_df.merge(real_df, on='url', how='inner', left_index=True, right_index=False)

        return df[["url", Config.GET_PREDICTED_FEATURE(), "predicted"]].as_matrix()


    def _get_df_real(self, hours):
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


    def _get_df_from_db(self, hours):
        """
        Возвращает все новости из таблицы news в формате DF, которые опубликованы в последние hours часов
        :param hours:
        """
        # Вычисляем время, за hours часов до текущего момента
        date = datetime.today() - timedelta(hours=hours)
        date = date.strftime('%Y-%m-%d %H:%M')

        db_values = list(self.db.get_news_after_date(date))
        df = pd.DataFrame(data=db_values, columns=["url", "predicted"])
        return df


    default_dict = {
            'vc.ru': 16,
            'tjournal.ru': 35,
            'forbes.ru': 20,
            'lenta.ru': 19,
            'lifenews.ru': 98,
            'meduza.io': 65,
            'navalny.com': 226,
            'ria.ru': 5,
            'roem.ru': 1,
            'slon.ru': 65,
            'vedomosti.ru': 9,
            'vesti.ru': 30,
        }

    def get_default_threshold(self, type):

        return Statistic.default_dict[type]