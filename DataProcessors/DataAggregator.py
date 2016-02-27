# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from math import log, sqrt, pow
import logging


class DataAggregator:

    def __init__(self, first_time, last_time, step=3):
        self.first_time = first_time
        self.last_time = last_time
        self.step = step


    def _general_apply(self, dataframe):
        """
        Проводит общие преобразования, не зависящие от FirstTime и LastTime
        :return:
        """
        dataframe = self._time_since_news(dataframe)
        dataframe = self._week_day(dataframe)
        dataframe = self._day_period(dataframe)
        dataframe = self._user_rank(dataframe)

        return dataframe


    def _time_since_news(self, dataframe):
        """
        Считаем время, между временем публикации записи и твитта
        :param dataframe:
        :return:
        """
        def diff_date_minutes(news_date, tweet_date):
            news_date = datetime.strptime(news_date, '%Y-%m-%d %H:%M')
            tweet_date = datetime.strptime(tweet_date, '%Y-%m-%d %H:%M:%S')
            return int((tweet_date-news_date).total_seconds()/60)

        dataframe["time_since_news"] = dataframe.apply(lambda s: diff_date_minutes(s["news_date"], s["created_at"]), axis=1)

        return dataframe


    def _week_day(self, dataframe):
        """
        Вычисляем день недели и кодируем его как one-hot encoding
        :param dataframe:
        :return:
        """

        week_days = ['is_monday','is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday']

        def get_week_day(date):
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            return week_days[date.weekday()]

        # one-hot encoding дня недели
        dataframe["week_day_news"] = dataframe.news_date.apply(lambda s: get_week_day(s))

        dataframe = pd.concat([dataframe, dataframe['week_day_news'].str.get_dummies()], axis=1)
        print dataframe.columns

        return dataframe


    def _day_period(self, dataframe):
        """
        Вычисляем период суток и кодируем как one-hot encoding
        :param dataframe:
        :return:
        """
        def get_minutes_since_midnight(date):
            midnight = date.split(" ")[0] + " 00:00"
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            midnight = datetime.strptime(midnight, '%Y-%m-%d %H:%M')
            return int((date-midnight).total_seconds()/60)

        # Сколько минут времени прошло с полуночи
        dataframe["minutes_since_midnight"] = dataframe.news_date.apply(lambda s: get_minutes_since_midnight(s))
        # Есть новости с ошибочной датой. Их исключаем
        dataframe = dataframe[dataframe["time_since_news"] > 0]

        # Функция разбивает время публикации новости на периоды
        def get_time_period(minutes):
            if (minutes > 1380 and minutes <= 1440) or (minutes <= 420): # 23-7
                return "is_night"

            elif (minutes > 420) and (minutes <= 780): # 7-13
                return "is_morning"

            elif (minutes > 780) and (minutes <= 1080): # 13-18
                return "is_day"

            elif (minutes > 1080) and (minutes <= 1380): # 18-23
                return "is_evening"

        dataframe["time_period"] = dataframe.minutes_since_midnight.apply(lambda s: get_time_period(s))
        dataframe = pd.concat([dataframe, dataframe['time_period'].str.get_dummies()], axis=1)


        return dataframe


    def _user_rank(self, dataframe):

        def user_rank(s):
            followers = float(s["user_followers_count"])
            favorites = float(s["user_favourites_count"])
            friends = float(s["user_friends_count"])
            statuses = float(s["user_statuses_count"])
            is_verified = float(s["user_verified"])
            is_retweet = float(s["is_retweet"])

            date_user_created = datetime.strptime(s["user_date_created"], '%Y-%m-%d %H:%M:%S')
            created_at = datetime.strptime(s["created_at"], '%Y-%m-%d %H:%M:%S')
            days_in_twitter = float((created_at-date_user_created).days)


            if statuses >= 0 and followers >= 0 and favorites >= 0 and statuses >= 0 and friends>=0:
                rank = sqrt(followers) * log(favorites+1, 2) * log(followers/(friends+1)+1, 5)# * sqrt(days_in_twitter/statuses)
            else:
                rank = 0


            #print followers, favorites, friends, days_in_twitter, statuses

            if is_verified:
                rank *= sqrt(2)

            if not is_retweet:
                rank *= sqrt(2)

            rank = pow(rank, 0.75)

            return rank

        dataframe["user_rank"] = dataframe.apply(lambda s: user_rank(s), axis=1)

        return dataframe


    def aggregate_first_time(self, news_df, tweets_df, df_columns):

        """
        Выполняет аггрегацию данных, для того, чтобы подготовить их к пригодному для обучения виду
        :param news_df:
        :param tweets_df:
        :param df_columns:
        :return:
        """
        dataframe = news_df.merge(tweets_df, on='url', left_index=True, right_index=False)#, how='outer')

        if len(dataframe) == 0:
            return dataframe

        # выполняем общие преобразования
        df = self._general_apply(dataframe)


        # считаем приращения показателей по шагам
        df = self._count_diff_by_step(df)


        # агрегируем получившиеся значения для First_time
        df = self._first_time_aggregate(df)

        #ONE HOT ENCODING
        df = self._one_hot_encoding(df, df_columns)

        """
        Был прецедент, когда почему-то tw_id были неуникальными! Важно узнать, нет ли такого больше
        ========================================================
        debug!!!
        """
        if sum(df.tw_id.duplicated()) > 0:
            logging.error("TW_ID DUPLICATED:\n{}".format(df[["url", "tw_id", "screen_name", "created_at", "news_date", "first_time_tweet"]]))
        """
        =========================================================
        """

        return df


    def _one_hot_encoding(self, df, df_columns):
        #ONE HOT ENCODING
        df.reset_index(inplace=True)
        dummy = pd.get_dummies(df['type'])
        df = df.join(dummy)

        # Заполняем пробелы в DF (отсуствующие колонки)
        for col in df_columns:
            if col not in df.columns:
                df[col] = pd.Series(np.zeros(len(df)))

        return df

    def _first_time_aggregate(self, df):
        """
        Суммируем все нужные показатели для времени first_time
        :param df:
        :return:
        """

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

        # Считаем суммарное количество верифицированных пользователей, которые твиттнули
        user_listed_count = pd.DataFrame(grouped["user_verified"].sum())
        user_listed_count.columns = ["user_verified_sum"]
        user_listed_count.reset_index(inplace=True)
        df = pd.merge(df, user_listed_count, on='url', left_index=True, right_index=False, how="outer")

        # Считаем суммарный рейтинг пользователей, твитнувших новость
        user_listed_count = pd.DataFrame(grouped["user_rank"].sum())
        user_listed_count.columns = ["user_rank_sum"]
        user_listed_count.reset_index(inplace=True)
        df = pd.merge(df, user_listed_count, on='url', left_index=True, right_index=False, how="outer")

        return df


    def _count_diff_by_step(self, df):
        """
        Функция считает приращение аудитории и твиттов по шагам
        :param df:
        :return:
        """

        x_features = []
        time = self.first_time

        # считаем абсолютные значения
        while(time > 0):

            ft_df = df[df["time_since_news"] <= time]
            grouped = ft_df.groupby("url")

            # Считаем общее количество твиттов
            count_of_tweets = pd.DataFrame(grouped["url"].count())
            feature_name = str(time)+"_time_tweet"
            x_features.append(feature_name)

            # бывают случаи, когда твиттов за период нет совсем => пустой df => ошибка при merge
            # Поэтому, для этого случая, созданим пустой dataframe (url, 0)
            if len(count_of_tweets)>0:
                count_of_tweets.columns = [feature_name]
                count_of_tweets[feature_name].fillna(value=0, inplace=True)
                count_of_tweets.reset_index(inplace=True)
            else:
                #print "ELSE"
                url_list = list(df.url.unique())
                zipped = zip(url_list, [0]*len(url_list))
                count_of_tweets = pd.DataFrame(zipped, columns=["url", feature_name])


            #print count_of_tweets#.columns
            #print df.columns

            df = pd.merge(df, count_of_tweets, on='url', left_index=True, right_index=False, how="outer")

            follower_sum = pd.DataFrame(grouped["user_followers_count"].sum())
            feature_name = str(time)+"_follower_sum"
            x_features.append(feature_name)

            # бывают случаи, когда твиттов за период нет совсем => пустой df => ошибка при merge
            # Поэтому, для этого случая, созданим пустой dataframe (url, 0)
            if len(follower_sum)>0:
                follower_sum.columns = [feature_name]
                follower_sum[feature_name].fillna(value=0, inplace=True)
                follower_sum.reset_index(inplace=True)
            else:
                url_list = list(df.url.unique())
                zipped = zip(url_list, [0]*len(url_list))
                follower_sum = pd.DataFrame(zipped, columns=["url", feature_name])

            df = pd.merge(df, follower_sum, on='url', left_index=True, right_index=False, how="outer")

            time -= self.step

        df = df.fillna(0)


        # считаем приращения
        time = self.first_time

        while(time > self.step):
            df["{}_time_tweet".format(time)] = df["{}_time_tweet".format(time)] - df["{}_time_tweet".format(time-self.step)]
            df["{}_follower_sum".format(time)] = df["{}_follower_sum".format(time)] - df["{}_follower_sum".format(time-self.step)]
            time -= self.step

        return df




    def aggregate_last_time(self, news_df, tweets_df):

        dataframe = news_df.merge(tweets_df, on='url', left_index=True, right_index=False)#, how='outer')

        df = self._general_apply(dataframe)

        # считаем, сколько времени прошло с момента публикации новости до текущего момента
        now = datetime.today()
        def time_from_now(news_date, now):
            news_date = datetime.strptime(news_date, '%Y-%m-%d %H:%M')
            return int((now-news_date).total_seconds()/60)

        df["time_from_now"] = df.news_date.apply(lambda s: time_from_now(s, now))

        # выбираем только те, которым уже исполнилось last_time минут
        df = df[df["time_from_now"] >= self.last_time]

        if (len(df) == 0):
            return None

        st_df = df[df["time_since_news"] <= self.last_time]

        if len(st_df) == 0:
            return None

        grouped = st_df.groupby("url")

        # Считаем число твиттов после Last Date
        count_of_tweets = pd.DataFrame(grouped["url"].count())
        count_of_tweets.columns = ["last_time_tweet"]
        count_of_tweets.reset_index(inplace=True)
        st_df = pd.merge(st_df, count_of_tweets, on='url', left_index=True, right_index=False, how="outer")


        # Считаем число ретвитов после LAST_TIME
        retweeted_count_sum = pd.DataFrame(grouped["is_retweet"].sum())
        retweeted_count_sum.columns = ["last_time_retweet"]
        retweeted_count_sum.reset_index(inplace=True)
        st_df = pd.merge(st_df, retweeted_count_sum, on='url', left_index=True, right_index=False, how="outer")


        return st_df