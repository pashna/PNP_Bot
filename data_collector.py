# -*- coding: utf-8 -*-
__author__ = 'popka'

import time
from DataProcessors.Engine import Engine
from DataBase.DB import DB
from datetime import datetime
from Config import GET_FIRST_TIME


if __name__ == '__main__':

    while (1):
        try:
            db = DB()
            engine = Engine(GET_FIRST_TIME())

            while(1):
                predicted = engine.predict()
                print "{}:    predicted = {}".format(datetime.today(), predicted)
                for pred in predicted:
                    # Если новость есть
                    if pred[0] != "":
                        db.insert_news(pred[0], pred[1])

                sleep_time = engine.syncClock()
                print "Спим {} секунд".format(sleep_time)
                time.sleep(sleep_time)

        except Exception as e:
            print e
