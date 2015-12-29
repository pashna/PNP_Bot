# -*- coding: utf-8 -*-
__author__ = 'popka'

import time
from telegram import Updater
import requests
import telegram
from DataProcessors.Engine import Engine
from Config import *
from DataBase.DB import DB

# TODO: Удаление файлов


if __name__ == '__main__':


    db = DB()
    """
    TOKEN = GET_TOKEN()
    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.addTelegramCommandHandler('start', start)
    dispatcher.addTelegramMessageHandler(answer)

    updater.start_polling()

    bot = telegram.Bot(token='token')
    """

    engine = Engine(15)

    while(1):
        predicted = engine.predict()
        print predicted
        for pred in predicted:
            # Если новость есть
            if pred[0] != "":
                db.insert_news(pred[0], pred[1])

        sleep_time = engine.syncClock()
        print "Спим {} секунд".format(sleep_time)
        time.sleep(sleep_time)

    """
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

    """