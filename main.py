# -*- coding: utf-8 -*-
__author__ = 'popka'
import time

from telegram import Updater
import requests
import telegram
from DataProcessors.Engine import Engine
from DataProcessors.NewsLoader import NewsLoader
from DataProcessors.TwitterLoader import TwitterLoader
from Config import *


def sendMessage(chat_id, text):
    requests.get("https://api.telegram.org/bot{}/sendmessage?chat_id={}&text={}".format(getToken(), chat_id, text))

my_set=set()

def start(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text="Привет, друг! Я буду рассказывать обо всем самом интересном")


def answer(bot, update):
    my_set.add(update.message.chat_id)
    bot.sendMessage(chat_id=update.message.chat_id, text="Прости, я тут вообще-то новости анализирую, мне некогда болтать")


if __name__ == '__main__':
    engine = Engine(15, 200)
    print engine.predict()



"""
if __name__ == '__main__':

    TOKEN = GET_TOKEN()
    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.addTelegramCommandHandler('start', start)
    dispatcher.addTelegramMessageHandler(answer)

    updater.start_polling()

    bot = telegram.Bot(token='token')

    while(1):
        for i in my_set:
            sendMessage(i, "ГЫ")

        print("sleep")
        time.sleep(5)

"""