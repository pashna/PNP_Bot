# -*- coding: utf-8 -*-
__author__ = 'popka'

import MySQLdb
import Config


class DB():
    """
    Соединение решил не закрывать. По моему опыту, так быстрее.
    """
    # Chats
    str_get_chat = "select id_chat from chats;"
    str_add_chat = "insert into chats (id_chat, user_id, user_name) VALUES ({}, '{}', '{}');"

    # News
    str_get_new_news = "select news_url, news_predicted from news where is_sended=0;"
    str_add_news = "insert into news (news_url, news_predicted, is_sended) VALUES ('{}', {}, 0);"
    str_mark_news_as_sent = "update news set is_sended=1 where news_url='{}';"


    def __init__(self):
        settings = Config.GET_DB_SETTING()

        self.db = MySQLdb.connect(host=settings["host"],
                     user=settings["user"],
                     passwd=settings["password"],
                     db=settings["db"])

        self.db.autocommit(True)
        self.cursor = self.db.cursor()


    def insert_news(self, url, predicted):
        """
        Добавляет новость в базу
        :param url:
        :param predicted:
        """
        add_string = DB.str_add_news.format(url, predicted)
        self.cursor.execute(add_string)


    def get_chats(self):
        """
        Возвращает список id всех чатов
        :return:
        """
        self.cursor.execute(DB.str_get_chat)
        rows = self.cursor.fetchall()
        return rows


    def mark_news_as_sent(self, url):
        """
        Помечает новость url, как отправленную
        :param url:
        """
        str = DB.str_mark_news_as_sent.format(url)
        self.cursor.execute(str)


    def get_new_news(self):
        """
        Возвращает список всех новостей, которые еще не были отправлены
        :return:
        """
        self.cursor.execute(DB.str_get_new_news)
        rows = self.cursor.fetchall()
        return rows


    def add_chat(self, chat_id, user_id, user_name):
        """
        Добавляет чат в базу
        :param chat_id:
        :param user_id:
        :param user_name:
        """
        try:
            str = DB.str_add_chat.format(chat_id, user_id, user_name)
            self.cursor.execute(str)
        except Exception:
            print "already in db"