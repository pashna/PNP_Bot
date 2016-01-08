# -*- coding: utf-8 -*-
__author__ = 'popka'

from DataProcessors.DataAggregator import DataAggregator
from DataProcessors.FileReader import FileReader
import Config
from datetime import datetime, timedelta
import pandas as pd
from utils.utils import get_news_type


class Statistic():

    def __init__(self, first_time, last_time, db):
        self.data_aggregator = DataAggregator(first_time, last_time)
        self.df_features = ["url", Config.GET_PREDICTED_FEATURE(), "news_date"]
        self.loader = FileReader()
        self.db = db


    def get_statistic(self, hours):
        result_matrix = self._get_real_and_predicted(hours)

        print(result_matrix)

        result = {"missed":0, "correct":0, "error": 0, "all": 0}

        for row in result_matrix:
            print row
            type = get_news_type(row[0])
            threshold = self.get_threshold(type)
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

            result["all"] += 1

        return result




    def _get_real_and_predicted(self, hours):
        real_df = self._get_df_real(hours)
        predicted_df = self._get_df_from_db(hours)
        #if predicted_df is not None or len(predicted_df)==0 or len(real_df)==0:
        #    return
        print real_df["url"]
        print "========="
        print predicted_df["url"]
        df = predicted_df.merge(real_df, on='url', how='inner', left_index=True, right_index=False)
        #print len(df)

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


    def get_threshold(self, type):
        dict = {
            'vc.ru': 40,
            'tjournal.ru': 80,
            'forbes.ru': 57,
            'lenta.ru': 48,
            'lifenews.ru': 375,
            'meduza.io': 342,
            'navalny.com': 461,
            'ria.ru': 8,
            'roem.ru': 1,
            'slon.ru': 247,
            'vedomosti.ru': 46,
            'vesti.ru': 159
        }

        return dict[type]