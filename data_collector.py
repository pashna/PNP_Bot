# -*- coding: utf-8 -*-
__author__ = 'popka'

import time
from DataProcessors.Engine import Engine
from DataBase.DB import DB
from datetime import datetime
from Config import GET_FIRST_TIME, GET_NEWS_FOLDER
import logging

if __name__ == '__main__':



#    time.sleep(400)
    logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG, filename=GET_NEWS_FOLDER() + "/" + "data_collector.log")
    while (1):
        try:
            db = DB()
            engine = Engine(GET_FIRST_TIME())

            while(1):
                predicted = engine.predict()
                logging.debug("{}:    predicted = {}".format(datetime.today(), predicted))
                for pred in predicted:
                    # Если новость есть
                    if pred[0] != "":
                        url = pred[0]
                        value = float("{0:.2f}".format(pred[1]))
                        news_date = pred[2]
                        firsttime_tweets = pred[3]
                        db.insert_news(url, value, news_date, firsttime_tweets)

                sleep_time = engine.syncClock()
                logging.debug("Sleeping  for {} seconds".format(sleep_time))
                time.sleep(sleep_time)


        except Exception as e:
            logging.exception("exception")
            #print "DataCollector Exception: {}".format(e)
