__author__ = 'popka'


import unittest
from BotEngine.Statistic import Statistic
from Config import *
from BotEngine.Chats import Chats
from BotEngine.Bot import Bot
from DataBase.DB import DB

class BotTests(unittest.TestCase):


    def metrics(self):

        db = DB()
        chats = Chats(db)
        bot = Bot(db, chats)
        #bot.send_message()

