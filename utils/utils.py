# -*- coding: utf-8 -*-
from urlparse import urlparse

__author__ = 'popka'


def get_news_type(url):
    url = url.replace("www.", "")

    if "://" in url:
        url = url.split("://")[1]

    url = url.split("/")[0]
    splited = url.split(".")

    return splited[-2] + "." + splited[-1]



def get_text_after_number(number, words_array):
    """
    Функция возвращает корректное значени склонения существительного после существительного
    :param number: чисительное
    :param words_array: массив существительных [час, часа, часов] ([1, 3, 11])
    :return:
    """

    number = number % 100

    if (number > 10 and number < 20) or (number%10 == 0):
        return words_array[2]

    if number % 10 == 1:
        return words_array[0]

    if number % 10 in [2,3,4]:
        return words_array[1]

    if number % 10 in [5,6,7,8,9]:
        return words_array[2]


def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z