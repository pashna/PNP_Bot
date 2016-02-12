# -*- coding: utf-8 -*-
__author__ = 'popka'
from BotEngine.Statistic import Statistic
from Config import GET_FIRST_TIME, GET_LAST_TIME, GET_LOGGER_FOLDER
from DataBase.DB import DB
from datetime import datetime, timedelta
import time
from Config import GET_LAST_TIME
import logging



if __name__ == '__main__':
    logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG, filename=GET_LOGGER_FOLDER() + "/" + "proofer.log")
    time.sleep(4*3600)
    while True:
        try:

            db = DB()
            statistic = Statistic(GET_FIRST_TIME(), GET_LAST_TIME(), db)
            date = datetime.today()
            #date = datetime.strptime("2016-02-12 02:25:29", "%Y-%m-%d %H:%M:%S")

            while True:

                logging.debug("new iteration")
                date += timedelta(hours=1)

                result_matrix = statistic.get_real_and_predicted(25)

                if result_matrix is not None:
                    logging.debug("updating {} news".format(len(result_matrix)))
                    for row in result_matrix:
                        url = row[0]
                        real = row[1]
                        db.set_real_value(url, real)

                else:
                    logging.debug("nothing to update")



                time_to_sleep = int ((date - datetime.today()).total_seconds())
                logging.debug("going to sleep for {}".format(time_to_sleep))
                time.sleep(time_to_sleep)


        except Exception as e:
            logging.exception("exception")
            time.sleep(600)
