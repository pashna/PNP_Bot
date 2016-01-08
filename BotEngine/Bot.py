# -*- coding: utf-8 -*-
__author__ = 'popka'

from telegram import Updater
import requests
import telegram
from Config import *
from Statistic import Statistic


class Bot():

    def __init__(self, db, chats):

        self.db = db
        self.chats = chats
        self.updater = Updater(token=GET_TOKEN())
        dispatcher = self.updater.dispatcher

        dispatcher.addTelegramCommandHandler('start', self.start)
        dispatcher.addTelegramCommandHandler('restrict', self.add_restrict)
        dispatcher.addTelegramCommandHandler('get_stat', self.stats)
        dispatcher.addTelegramMessageHandler(self.answer)

        self.updater.start_polling()
        self.bot = telegram.Bot(token=GET_TOKEN())

        self.statistician = Statistic(GET_FIRST_TIME(), GET_LAST_TIME(), db)


    def start(self, bot, update):
        message = update['message']

        user_id = message['from_user']['id']
        user_name = message['from_user']['first_name']

        chat_id = message['chat']['id']

        self.chats.add_chat(chat_id, user_id, user_name)
        bot.sendMessage(chat_id=update.message.chat_id, text="Привет, друг! Я буду рассказывать обо всем самом интересном")


    def stats(self, bot, update):
        try:
            message = update['message']
            chat_id = update.message.chat_id
            text = message['text']
            text = text.split(' ')
            hours = int(text[1])

            stat = self.statistician.get_statistic(hours)
            text = "За {} час. было предсказано {} новостей. \nИз них топовых было {}. \nПравильно предсказанно топовых {}. \nОшибочно предсказанно топовых {} \nПропущено топовых"
            text = text.format(hours, stat["all"], stat["correct"]+stat["missed"], stat["correct"], stat["error"], stat["missed"])

            bot.sendMessage(chat_id=chat_id, text=text)

        except Exception as e:
            print e
            bot.sendMessage(chat_id=update.message.chat_id, text="Не понял. Введи, например\n /stats 3")


    def add_restrict(self, bot, update):

        try:
            message = update['message']
            chat_id = update.message.chat_id
            text = message['text']
            text = text.split(' ')
            type = text[1]
            value = float(text[2])
            self.chats.add_restrict(chat_id, type, value)
            bot.sendMessage(chat_id=chat_id, text="Установил для {} ограничение в {} твиттов".format(type, value))

        except Exception as e:
            print e
            bot.sendMessage(chat_id=update.message.chat_id, text="Не понял. Введи, например\n /restrict lenta.ru 5")


    def answer(self, bot, update):
        """
        Отвечает пользователям на их сообщения
        :param bot:
        :param update:
        """
        bot.sendMessage(chat_id=update.message.chat_id, text="Прости, я тут вообще-то новости анализирую, мне некогда болтать")


    def send_message(self, chats, url, predicted):
        text = "Ухты! Ты только глянь, какая новость! Наберет {} твиттов! {}".format(int(predicted), url)
        for chat_id in chats:
            requests.get("https://api.telegram.org/bot{}/sendmessage?chat_id={}&text={}".format(GET_TOKEN(), chat_id, text))