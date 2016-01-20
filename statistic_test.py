# -*- coding: utf-8 -*-
__author__ = 'popka'

from BotEngine.Statistic import Statistic
from DataBase.DB import DB
from BotEngine.Chats import Chats
from BotEngine.Bot import Bot
from DataBase.DB import DB

"""
db = DB()
st = Statistic(15, 180, db)
print st.get_statistic(4, {"lifenews.ru": 30})
"""

db = DB()
chats = Chats(db)
bot = Bot(db, chats)