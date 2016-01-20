# -*- coding: utf-8 -*-
__author__ = 'popka'

from telegram import Updater
import requests
import telegram
from Config import *
from Statistic import Statistic
import sys, traceback
from utils.utils import get_text_after_number, merge_two_dicts


class Bot():

    def __init__(self, db, chats):

        self.db = db
        self.chats = chats
        self.updater = Updater(token=GET_TOKEN())
        dispatcher = self.updater.dispatcher

        dispatcher.addTelegramCommandHandler('start', self.start)
        dispatcher.addTelegramCommandHandler('restrict', self.add_restrict)
        dispatcher.addTelegramCommandHandler('stat', self.stats)
        dispatcher.addTelegramCommandHandler('info', self.info)
        dispatcher.addTelegramCommandHandler('help', self.help)
        dispatcher.addTelegramCommandHandler('sites', self.sites)
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
        bot.sendMessage(chat_id=update.message.chat_id, text="Привет, друг! Я буду рассказывать обо всем самом интересном. \nЧтобы мы смогли лучше узнать друг друга, используй команду /help")


    def stats(self, bot, update):
        try:
            message = update['message']
            chat_id = update.message.chat_id
            text = message['text']
            text = text.split(' ')
            hours = int(text[1])

            if hours > 48:
                text = "Не больше 48 часов"
                bot.sendMessage(chat_id=chat_id, text=text)
                return

            hours_text = get_text_after_number(hours, ["час", "часа", "часов"])

            stat = self.statistician.get_statistic(hours, self.chats.chats[chat_id])

            if stat is None:
                text = "У меня нет информации за последние {} {}.".format(hours, hours_text)
                bot.sendMessage(chat_id=chat_id, text=text)
                return

            news_text = get_text_after_number(stat["all"], ["новость", "новости", "новостей"])

            text = "За последние {} {} было предсказано {} {}. " \
                   "\nПравильно предсказано топовых {} из {}. " \
                   "\nОшибочно предсказанно топовых {}. " \
                   "\nОшибочно пропущено топовых {}." \
                   "\nПравильно отсеяно непопулярных {}."

            text = text.format(hours, hours_text, stat["all"], news_text,\
                               stat["correct"], stat["correct"]+stat["missed"], \
                               stat["error"], \
                               stat["missed"],\
                               stat["filtered"]
                               )

            # Добавляем информацию о порогах
            text += "\n\n"
            text += "Статистика была расчитана по порогам:"
            threshold = merge_two_dicts(Statistic.default_dict, self.chats.chats[chat_id])
            for key, value in threshold.iteritems():
                text +="\n"
                text += str(int(value)) + " : " + str(key)

            bot.sendMessage(chat_id=chat_id, text=text)



        except Exception as e:
            print e
            traceback.print_exc(file=sys.stdout)

            bot.sendMessage(chat_id=update.message.chat_id, text="Не понял. Введи, например\n /stat 15\n - статистика за 15 часов\nВозможно, промежуток времени недостаточный")


    def add_restrict(self, bot, update):

        try:
            message = update['message']
            chat_id = update.message.chat_id
            text = message['text']
            text = text.split(' ')
            type = text[1]
            value = int(text[2])

            # Если такого сайта нет, сообщаем об этом
            if type not in Statistic.default_dict.keys():
                text = "Такого сайта нет. Выбирай любой из:\n"
                for key in Statistic.default_dict.keys():
                    text += key
                    text += "\n"
                bot.sendMessage(chat_id=update.message.chat_id, text=text)
                return

            self.chats.add_restrict(chat_id, type, value)

            tweets_text = get_text_after_number(value, ["твит", "твита", "твитов"])
            bot.sendMessage(chat_id=chat_id, text="Установил для {} ограничение в {} {}".format(type, value, tweets_text))

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


    def send_message(self, chats, url, predicted, first_time_tweets):
        #text = "Ухты! Ты только глянь, какая новость! Наберет {} твиттов! {}".format(int(predicted), url)

        first_time_text = get_text_after_number(GET_FIRST_TIME(), ["минута", "минуты", "минут"])
        last_time_text = get_text_after_number(GET_LAST_TIME(), ["минута", "минуты", "минут"])
        first_time_tweets_text = get_text_after_number(first_time_tweets, ["твит", "твита", "твитов"])
        predicted_time_tweets_text = get_text_after_number(int(predicted), ["твит", "твита", "твитов"])

        text = "{}\n" \
               "За первые {} {} новость уже набрала {} {}. \n" \
               "Через {} {} она наберет примерно {} {}."\
            .format(url, \
                    GET_FIRST_TIME(), first_time_text,first_time_tweets, first_time_tweets_text,  \
                    GET_LAST_TIME(), last_time_text, int(predicted), predicted_time_tweets_text)

        for chat_id in chats:
                requests.get("https://api.telegram.org/bot{}/sendmessage?chat_id={}&text={}".format(GET_TOKEN(), chat_id, text))


    def info(self, bot, update):

        try:
            message = update['message']
            chat_id = update.message.chat_id

            chats = self.chats.chats[chat_id]

            chats_text = ""
            for key, value in chats.iteritems():

                chats_text += "\n"
                chats_text += str(int(value)) + "  :  " + str(key)

            if len(chats_text) == 0:
                chats_text = "\nВы получаете все сообщения. Ограничений на количество новостей нет.\n\nЧтобы поставить, воспользуйтейсь командой /restrict lenta.ru 10"
                bot.sendMessage(chat_id=chat_id, text=chats_text)
                return

            bot.sendMessage(chat_id=chat_id, text="Текущие пороги, в соотвествии с которыми Вы получаете сообщения:{}".format(chats_text))

        except Exception as e:
            print e
            bot.sendMessage(chat_id=update.message.chat_id, text="Что-то пошло не так. Мне кажется я заболел. Попробуй позже.")


    def help(self, bot, update):
        try:
            message = update['message']
            chat_id = update.message.chat_id

            text = "Что же я умею:\n\n" \
                   "Команда /restrict lenta.ru 10\n- высылать с сайта lenta.ru только те новости, которые наберут больше 10 твитов\n\n" \
                   "Команда /stat 10\n- выдает статистику моей работы за последние 10 часов\n\n" \
                   "Команда /info\n- покажет текущие пороги, по которым я высылаю новости \n\n" \
                   "Команда /sites\n- покажет список сайтов, для которых я умею предсказывать популярность новостей \n\n" \
                   "А теперь прости, мне нужно работать"


            bot.sendMessage(chat_id=chat_id, text=text)

        except Exception as e:
            print e
            bot.sendMessage(chat_id=update.message.chat_id, text="Что-то пошло не так. Мне кажется я заболел. Попробуй позже.")


    def sites(self, bot, update):
        try:
            message = update['message']
            chat_id = update.message.chat_id

            text = "Я неплохо предсказываю:\n"
            for site in Statistic.default_dict.keys():
                text += "\n"
                text += site

            bot.sendMessage(chat_id=chat_id, text=text)

        except Exception as e:
            print e
            bot.sendMessage(chat_id=update.message.chat_id, text="Что-то пошло не так. Мне кажется я заболел. Попробуй позже.")
