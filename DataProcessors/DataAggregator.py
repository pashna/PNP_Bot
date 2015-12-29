# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


class DataAggregator:

    def __init__(self, first_time):
        self.first_time = first_time


    def _prepare(self, news_df, tweets_df):
        """
        Функция считывает данные и производит с ними общие преобразования. Вычисляет:
        1) количество минут с момента публикации записи до данного твита
        2) номер дня недели новости
        3) время с полуночи новости

        :param news_file: путь к файлу с новостями
        :param tweets_file: путь к файлу с твиттами
        """
        # Считываем и мерджим данные
        #print "BEFORE MERGED = ", len(news_df), len(tweets_df)


        # Общие преобразования
#        dataframe = self._general_apply(dataframe)

        # Фильтруем новости с ошибочной датой и удаляем очень ранние и очень поздние
        #self._filter_news(dataframe)

#        return dataframe


    def _general_apply(self, dataframe):

        """
        Проводит общие преобразования, не зависящие от FirstTime и LastTime
        :return:
        """

        def diff_date_minutes(news_date, tweet_date):

            news_date = datetime.strptime(news_date, '%Y-%m-%d %H:%M')
            tweet_date = datetime.strptime(tweet_date, '%Y-%m-%d %H:%M:%S')

            print "{} - {} = {}".format(tweet_date, news_date, int((tweet_date-news_date).total_seconds()/60))
            return int((tweet_date-news_date).total_seconds()/60)

        def get_week_day(date):
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            return date.weekday()

        def get_minutes_since_midnight(date):
            midnight = date.split(" ")[0] + " 00:00"
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            midnight = datetime.strptime(midnight, '%Y-%m-%d %H:%M')
            return int((date-midnight).total_seconds()/60)

        dataframe["time_since_news"] = dataframe.apply(lambda s: diff_date_minutes(s["news_date"], s["created_at"]), axis=1)
        dataframe["week_day_news"] = dataframe.news_date.apply(lambda s: get_week_day(s))
        dataframe["minutes_since_midnight"] = dataframe.news_date.apply(lambda s: get_minutes_since_midnight(s))

        return dataframe



    def _get_hours_before(self, date, hours=3):
            """
            "Перевод часов"
            Функция возвращает дату-строку, за hours часов до даты date
            Если нужно перевести стрелки часов вперед - передаем со знаком минус
            :param date:
            :param hours:
            :return:
            """
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            d = date - timedelta(hours=hours)
            d = str(d).split(":")
            d = d[0]+":"+d[1]
            return d

    """
    def _filter_news(self, dataframe):

        # Есть новости с ошибочной датой. Их исключаем
        dataframe = dataframe[dataframe["time_since_news"] > 0]

        # Отсечем новости в первые и в последние n_hours часов
        n_hours = 8
        min_date = dataframe.news_date.min()
        max_date = dataframe.news_date.max()

        first_date =  self._get_hours_before(min_date, -n_hours)
        last_date = self._get_hours_before(max_date, n_hours)

        dataframe = dataframe[dataframe["news_date"] > first_date]
        dataframe = dataframe[dataframe["news_date"] < last_date]
    """

    def aggregate(self, news_df, tweets_df, df_columns):

        """
        Выполняет аггрегацию данных, для того, чтобы подготовить их к пригодному для обучения виду
        :param news_df:
        :param tweets_df:
        :param df_columns:
        :return:
        """

        dataframe = news_df.merge(tweets_df, on='url', left_index=True, right_index=False)

        if len(dataframe) == 0:
            return dataframe

        df = self._general_apply(dataframe)

        #======= Для First Time
        # сплошным текстом аггрегирую все, что нужно
        ft_df = df[df["time_since_news"] <= self.first_time]
        grouped = ft_df.groupby("url")

        # Считаем общее количество твиттов
        count_of_tweets = pd.DataFrame(grouped["url"].count())
        count_of_tweets.columns = ["first_time_tweet"]
        count_of_tweets.reset_index(inplace=True)
        df = pd.merge(df, count_of_tweets, on='url', left_index=True, right_index=False, how="outer")

        # Считаем общую аудиторию
        follower_sum = pd.DataFrame(grouped["user_followers_count"].sum())
        follower_sum.columns = ["follower_sum"]
        follower_sum.reset_index(inplace=True)
        df = pd.merge(df, follower_sum, on='url', left_index=True, right_index=False, how="outer")

        # Считаем число ретвитов
        retweeted_count_sum = pd.DataFrame(grouped["is_retweet"].sum())
        retweeted_count_sum.columns = ["retweeted_count_sum"]
        retweeted_count_sum.reset_index(inplace=True)
        df = pd.merge(df, retweeted_count_sum, on='url', left_index=True, right_index=False, how="outer")

        # Считаем общее число списков, в которых состоят сделавшие посты
        user_listed_count = pd.DataFrame(grouped["user_listed_count"].sum())
        user_listed_count.columns = ["user_listed_count_sum"]
        user_listed_count.reset_index(inplace=True)
        df = pd.merge(df, user_listed_count, on='url', left_index=True, right_index=False, how="outer")

        """
        #======= Для Second Time

        st_df = df[df["time_since_news"] <= self.last_time]

        grouped = st_df.groupby("url")

        # Считаем число твиттов после Last Date
        count_of_tweets = pd.DataFrame(grouped["url"].count())
        count_of_tweets.columns = ["last_time_tweet"]
        count_of_tweets.reset_index(inplace=True)
        df = pd.merge(df, count_of_tweets, on='url', left_index=True, right_index=False, how="outer")

        # Считаем число ретвитов после LAST_TIME
        retweeted_count_sum = pd.DataFrame(grouped["is_retweet"].sum())
        retweeted_count_sum.columns = ["last_time_retweet"]
        retweeted_count_sum.reset_index(inplace=True)
        df = pd.merge(df, retweeted_count_sum, on='url', left_index=True, right_index=False, how="outer")
        """

        # Бинаризируем type
        df.reset_index(inplace=True)
        dummy = pd.get_dummies(df['type'])
        df = df.join(dummy)

        # Заполняем пробелы в DF (отсуствующие колонки)
        for col in df_columns:
            if col not in df.columns:
                df[col] = pd.Series(np.zeros(len(df)))

        print df.columns
        return df


