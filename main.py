# -*- coding: utf-8 -*-
__author__ = 'popka'
import time

from telegram import Updater
import requests
import telegram
from DataProcessors.Engine import Engine
from datetime import datetime, timedelta
from Config import *

# TODO: Удаление файлов

def sendMessage(chat_id, text):
    requests.get("https://api.telegram.org/bot{}/sendmessage?chat_id={}&text={}".format(GET_TOKEN(), chat_id, text))

my_set=set()

def start(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text="Привет, друг! Я буду рассказывать обо всем самом интересном")


def answer(bot, update):
    my_set.add(update.message.chat_id)
    bot.sendMessage(chat_id=update.message.chat_id, text="Прости, я тут вообще-то новости анализирую, мне некогда болтать")


if __name__ == '__main__':

    TOKEN = GET_TOKEN()
    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.addTelegramCommandHandler('start', start)
    dispatcher.addTelegramMessageHandler(answer)

    updater.start_polling()

    bot = telegram.Bot(token='token')



    engine = Engine(15)

    while(1):
        predicted = engine.predict()
        print predicted
        for pred in predicted:
            print pred
            if pred[1] > 2:
                for chat_id in my_set:
                    print "sended"
                    sendMessage(chat_id, "Гля, какая новость! Наберет {} твиттов! {}".format(int(pred[1]), pred[0]))

        sleep_time = engine.syncClock()
        print "Спим {} секунд".format(sleep_time)
        time.sleep(sleep_time)

