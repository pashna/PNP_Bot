# -*- coding: utf-8 -*-
__author__ = 'popka'
from BotEngine.Statistic import Statistic

class Chats:

    def __init__(self, db):
        self.db = db
        self.chats = self._get_chats()


    def _get_chats(self):
        """
        Возвращает словарь всех чатов из базы
        :return:
        """
        chats = self.db.get_chats()
        chat_list = {}
        for chat in chats:
            chat_list[chat[0]] = {}

        print chat_list
        return chat_list


    def add_restrict(self, chat_id, type, count):
        """
        Функция задает ограничение на урлы типа type для чата chat_id, равное count
        :param chat_id:
        :param type:
        :param count:
        """
        self.chats[chat_id][type] = count


    def add_chat(self, chat_id, user_id, user_name):
        """
        Добавляет новый чат в словарь и в базу
        :param chat_id:
        :param user_id:
        :param user_name:
        """
        self.chats[chat_id] = Statistic.default_dict
        self.db.add_chat(chat_id, user_id, user_name)


    def get_relevant_chats(self, type, predicted):
        """
        Возвращает список id чатов, для которых ограничения на type либо нет, либо оно меньше predicted
        :param type:
        :param predicted:
        :return:
        """
        relevant_chats = []

        for chat_id, restriction in self.chats.iteritems():
            if not restriction.has_key(type):
                relevant_chats.append(chat_id)
            else:
                if restriction[type] < predicted:
                    relevant_chats.append(chat_id)

        return relevant_chats