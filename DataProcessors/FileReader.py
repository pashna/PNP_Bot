# -*- coding: utf-8 -*-

__author__ = 'popka'


import shutil
import pandas as pd
import numpy as np
from os import listdir, remove
from os.path import isfile, join

class FileReader():

    def _get_files_in_folder(self, folder):
        """
        Возвращает список всех файлов в папке
        :param folder:
        :return: list(files in folder)
        """
        files_in_folder = [f for f in listdir(folder) if isfile(join(folder, f))]
        return files_in_folder


    def _drop_duplicates(self, df, column):
        """
        Удаляет повторяющиеся строки DF по колонке column
        :param df:
        :param column:
        :return: df
        """
        last_size = len(df)
        dupl = df[column].duplicated()
        dupl = np.invert((dupl.as_matrix()))
        df = df[dupl]
        print "удалено ", last_size-len(df)
        return df


    def get_concated_files(self, folder, drop_column, last_file_name):
        """
        Функция возвращает DF, собранный из файлов, которые лежат в папке folder.
        Перед возвращением, она удаляет все строки, в которых drop_column дублируется
        :param folder: папка
        :type folder: str
        :param drop_column: колонка, по которое дропаем
        :param last_file_name: имя последнего сохраненного файла
        :return: dataframe, max_file_name
        """
        files_in_folder = self._get_files_in_folder(folder)

        for i in range(len(files_in_folder)):
            files_in_folder[i] = folder + "/" +files_in_folder[i]


        df = None
        #print last_file_name
        for file in files_in_folder:

            # образаем полный путь
            if file.split("/")[-1] > last_file_name:

                n_df = pd.read_csv(file, sep=",")
                if df is None:
                    df = n_df
                else:
                    df = df.append(n_df)

        df = self._drop_duplicates(df, drop_column)
        df.reset_index(inplace=True, drop=True)

        return df, max(files_in_folder)


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
