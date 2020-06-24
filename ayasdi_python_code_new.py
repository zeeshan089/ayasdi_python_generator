#encoding utf-8
import csv
import datetime
# from datetime import datetime
import threading
import math
import random
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
from statistics import mean, stdev
from random_words import RandomWords
import sys, traceback
import sqlite3
from collections import OrderedDict

###################################################################################################################
#Class which will take care of generating random data for million rows
class Data_generator:


    #Method to calculate Gaussian Distribution of array of values
    # def gaussian_transform( self, arr, transformed_list = []):
    #
    #     self.arr = list(arr)
    #     arr_mean = mean(list(arr))
    #
    #     std_dev = stdev(list(arr))
    #     a = 0
    #     pi = math.pi
    #     for i in arr:
    #         a = ((i - arr_mean) ** 2) / (2 * (std_dev ** 2))
    #         e = math.exp(-a)
    #         transformed_list.append((1 / (std_dev * math.sqrt(2 * pi))) * e)
    #
    #     return transformed_list

    def gaussian_transform(self, i, arr_mean, std_dev):

        pi = math.pi

        a = ((i - arr_mean) ** 2) / (2 * (std_dev ** 2))
        e = math.exp(-a)
        return (1 / (std_dev * math.sqrt(2 * pi))) * e



    ## Following methods make the container object iterable
    def next(self):
        if not self.arr:
            raise StopIteration
        return self.arr.pop()

    def __iter__(self):
        return self


class Gaussian_datagen_driver:

    def generate_gaussian_millon(self,manager_gaussian_list, manager_header_list):
        creator_object = Data_generator()

        for i in range(2,11):

            million_val_arr = random.sample(range(i, 1000000), 900000)
            arr_mean = mean(million_val_arr)
            std_dev = stdev(million_val_arr)
            gaussian_return = [creator_object.gaussian_transform(i, arr_mean, std_dev) for i in million_val_arr]

            ##This array will be used to introduce 10% noise in the column
            ten_percent_null_array = [None] * 100000
            gaussian_with_null = gaussian_return + ten_percent_null_array
            random.shuffle(gaussian_with_null)
            header_val = "col" + str(i) + "_" + str(arr_mean)
            manager_header_list.append(str(header_val))
            manager_gaussian_list.append(gaussian_with_null)


        return manager_gaussian_list, manager_header_list


###################################################################################################################


class csv_rw:
    def write_csv(self, lists, header, filename):
        try:
            with open(filename, 'w') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(header)
                writer.writerows(lists)
                return True
        except :
            #return False
            traceback.print_exc(file=sys.stdout)



#################################################################################################################
# class Random_words_gen:
#
#     def generate_words(self,upper_limit, million_words_array = []):
#         self.rw = RandomWords()
#         for _ in range(int(upper_limit)):
#             word = self.rw.random_word()
#             million_words_array.append(word)
#         return million_words_array

class Million_words_driver:

    def generate_million_words(self, manager_arr):
        self.rw = RandomWords()
        for i in range(1,10):
            million_words_return = [self.rw.random_word() for _ in range(900000)]

            ##This array will be used to introduce 10% noise in the row
            ten_percent_null_array = [None] * 100000
            million_words_with_null = million_words_return + ten_percent_null_array
            random.shuffle(million_words_with_null)
            manager_arr.append(million_words_with_null)


        return manager_arr

###################################################################################################################
class date_generator:

    def gen_date(self):

        start_date = datetime.date(2014, 1, 1)
        end_date = datetime.date(2014, 12, 31)

        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        return random_date

class Date_gen_driver:

    def get_million_dates(self,manager_arr):
        date_gen_object = date_generator()
        for _ in range(1000000):
            manager_arr.append(str(date_gen_object.gen_date()))
        # manager_arr = [str(date_gen_object.gen_date()) for _ in range(1000000)]

        return manager_arr

##############################################################################################################






##Driver code
if __name__ == '__main__':
    ##For checking the execute time of this script
    print(datetime.datetime.now().time())

    manager = Manager()

    manager_arr_for_words = []
    manager_for_gaussian_lists = []
    manager_list_headers = []
    manager_arr_for_dates = []
    manager_arr_for_words = manager.list(manager_arr_for_words)
    manager_for_gaussian_lists = manager.list(manager_for_gaussian_lists)
    manager_arr_for_dates = manager.list(manager_arr_for_dates)
    manager_list_headers = manager.list(manager_list_headers)


    gaussian_object = Gaussian_datagen_driver()
    random_words_object = Million_words_driver()
    million_dates_object = Date_gen_driver()

    pool = ThreadPool(processes=3)

    async_gaussian_random = pool.apply_async(gaussian_object.generate_gaussian_millon(manager_for_gaussian_lists, manager_list_headers))
    async_million_words = pool.apply_async(random_words_object.generate_million_words(manager_arr_for_words))
    async_million_dates = pool.apply_async(million_dates_object.get_million_dates(manager_arr_for_dates))

    pool.close()
    pool.join()

    ## Now the computation part is done. Following code will format data and write it to the CSV/Sqlite

    pid = list(range(1,1000001))
    header_row = ["col1"]

    word_and_date_lists = ["col11","col12","col13","col14","col15","col16","col17","col18","col19","col_dates"]
    header_row = header_row + list(manager_list_headers) +  word_and_date_lists
    print(header_row)
    rows = [pid] + list(manager_for_gaussian_lists) + list(manager_arr_for_words) + [manager_arr_for_dates]
    rows = zip(*rows)

################################################################################################################

    header_string_for_db = ','.join(header_row)

    con = sqlite3.connect(":memory:")

    # Create the table
    con.execute("create table ayasdi(" + header_string_for_db +  ")")

    # Fill the table
    con.executemany("insert into ayasdi(" + header_string_for_db +  ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

#################################################################################################################

    #Uncomment the following lines if you wish to write to a CSV file
    # csv_writer = csv_rw()
    # writer_return = csv_writer.write_csv(rows,header_row,"/home/hackerearth/Desktop/ayasdi.csv")
    # print(writer_return)
    print(datetime.datetime.now().time())




