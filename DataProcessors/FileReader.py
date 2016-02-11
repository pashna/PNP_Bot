# -*- coding: utf-8 -*-

__author__ = 'popka'


import shutil
import pandas as pd
import numpy as np
from os import listdir, remove
from os.path import isfile, join
import os
from datetime import datetime, timedelta

class FileReader():

    def _get_files_in_folder(self, folders):
        """
        Возвращает список всех файлов в папке
        :param folder:
        :return: list(files in folder)
        """

        files_in_folder = []

        for folder in folders:
            for f in listdir(folder):
                files_in_folder.append(join(folder, f))

        return files_in_folder


    def _drop_duplicates(self, df, column):
        """
        Удаляет повторяющиеся строки DF по колонке column
        :param df:
        :param column:
        :return: df
        """
        dupl = df[column].duplicated()
        dupl = np.invert((dupl.as_matrix()))
        df = df[dupl]
        return df


    def _get_folder_name(self, date, folder, hours):
        """
        Функция возвращает папки, в которых нужно искать, в соответствии с датой.
        За текущий час и за предыдущие hours. Какой кошмар, Боже, за что?
        :param date:
        :param folder: корнеая папка (новости или твитты)
        :param hours: количество часов, в течении которых мы ищем папки (в прошлом).
        """
        # вычисляем название папкок, в которой нужно искать новые файлы
        folders = []

        # Вычисляем имя старой папки
        for hour in range(hours+1):
            date_hour_ago = date - timedelta(hours=hour)
            past_date_folder_name = date_hour_ago.strftime('%Y_%m_%d_%H')
            past_folder = folder + "/" + past_date_folder_name

            if os.path.exists(past_folder):
                folders.append(past_folder)

        return folders



    def get_concated_files(self, folder, date, drop_column, last_file_name):
        """
        Функция возвращает DF, собранный из файлов, которые лежат в папке folder.
        Перед возвращением, она удаляет все строки, в которых drop_column дублируется
        :param folder: папка
        :type folder: str
        :param date: дата, для того, чтобы понять, в какой папке искать
        :param drop_column: колонка, по которое дропаем
        :param last_file_name: имя последнего сохраненного файла
        :return: dataframe, max_file_name
        """
        folders = self._get_folder_name(date, folder, 1)

        files_in_folder = self._get_files_in_folder(folders)

        df = None

        for file in files_in_folder:

            # образаем полный путь
            if file.split("/")[-1] > last_file_name.split("/")[-1]:

                n_df = pd.read_csv(file, sep=",")
                if df is None:
                    df = n_df
                else:
                    df = df.append(n_df)

        if df is not None:
            df = self._drop_duplicates(df, drop_column)
            df.reset_index(inplace=True, drop=True)
            return df, max(files_in_folder)

        else:
            return None, last_file_name


    def remove_files(self, folder, last_file_name):
        """
        Функция удаляет все файлы, которые появились раньше last_file_name
        :param folder:
        :param last_file_name:
        """
        files_in_folder = self._get_files_in_folder(folder)

        for file in files_in_folder:
            if file < last_file_name.split("/")[-1]:
                remove(folder + "/" + file)


    def get_concat_files_by_hours(self, folder, hours, drop_column):
        """
        Функция возвращает df, собранный из csv корневой папки folder, для папок в течение hours.
        Убирает дубликаты по колонке drop_column
        :param folder:
        :param hours:
        :param drop_column:
        :return:
        """
        folders = self._get_folder_name(datetime.today(), folder, hours)
        files_in_folder = self._get_files_in_folder(folders)

        df = None
        for file in files_in_folder:
            n_df = pd.read_csv(file, sep=",")

            if df is None:
                df = n_df
            else:
                df = df.append(n_df)

        if df is not None:
            df = self._drop_duplicates(df, drop_column)
            df.reset_index(inplace=True, drop=True)

        return df