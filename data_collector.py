# -*- coding: utf-8 -*-
__author__ = 'popka'

import time
from DataProcessors.Engine import Engine
from DataProcessors.PredictedHandler import PredictedHandler
from DataBase.DB import DB
from datetime import datetime
from Config import GET_FIRST_TIME, GET_NEWS_FOLDER, GET_LOGGER_FOLDER
import logging

if __name__ == '__main__':


    time.sleep(400)
    logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG, filename=GET_LOGGER_FOLDER() + "/" + "data_collector.log")

    while (1):
        try:
            db = DB()
            engine = Engine(GET_FIRST_TIME())
            predicted_handler = PredictedHandler(db)

            while(1):
                # делаем предсказания
                predicted = engine.predict()
                logging.debug("{}:    predicted = {}".format(datetime.today(), predicted))

                # отправляем нужные в базу
                predicted_handler.handle_predicted(predicted)

                # спим необходимое время
                sleep_time = engine.syncClock()
                logging.debug("Sleeping  for {} seconds".format(sleep_time))
                time.sleep(sleep_time)


        except Exception as e:
            logging.exception("exception")

