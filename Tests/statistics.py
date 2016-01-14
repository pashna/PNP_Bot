__author__ = 'popka'


import unittest
from BotEngine.Statistic import Statistic
from Config import *
from DataBase.DB import DB

class StatisticTests(unittest.TestCase):


    def metrics(self):

        db = DB()

        st = Statistic(GET_FIRST_TIME(), GET_LAST_TIME(), db)
        stat = st.get_statistic(hours=150, restriction={"lenta.ru": 10})
        print stat
