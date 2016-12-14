#!/usr/bin/python
# -*- coding: utf-8 -*-
# Python 3.3

import sys
from pymongo import MongoClient
import ast


def convert(object0):
    for i, item in enumerate(object0):
        if type(item) == dict:
            # получаем значение поля request_body
            request = item['request_body']
            # конвертируем в словарь
            try:
                request_dict = ast.literal_eval(request)
            except LookupError:
                request_dict = None
            # получаем запрос
            try:
                terms_value = request_dict['terms'][0]['value']
            except LookupError:
                print(terms_value)
                terms_value = None
            # составляем словарь на основе запросов
            try:
                # игнорируем списки
                if not type(terms_value) == list:
                    terms_value = str(terms_value)
                    if terms_value in clist:
                        clist[terms_value] += 1
                    else:
                        clist[terms_value] = 1
            except TypeError:
                terms_value = None
    return clist
    # print(clist)


if __name__ == '__main__':

    # проверка аргументов
    if len(sys.argv) < 3:
        print("""Missing ip address console param. Examples:
        python mongo_aggregate.py 192.168.0.100 statistics data-2016-05""")
        sys.exit(0)

    user_host = sys.argv[1]
    user_db = sys.argv[2]
    user_collection = sys.argv[3]

    # хост MongoDB
    host = user_host
    client = MongoClient[host]
    db = client[user_db]
    coll = db[user_collection]

    # данные для запроса
    id = ''
    name = ''

    # запрос для выборки и количество объектов
    req1 = {'request_body': {'$regex': 'id'}}
    req2 = {'request_body': {'$regex': 'name'}}
    find_text = {'$and': [req1, req2]}
    lenght = coll.count(find_text)
    print("Found", lenght, "objects")

    count = 0
    objects = []
    clist = {}

    print("Data export starting.\n")
    # составляем список экспортируемых объектов
    for i, item in enumerate(coll.find(find_text)):
        objects.append(item)
    print("Data export complete.\n")

    # получение словаря
    clist = convert(objects)
    print("Data convertation complete.\n")

    # сортировка словаря по значению в порядке уменьшения
    sort_list = list(clist.items())
    for item in sort_list:
        sort_list.sort(key=lambda item: item[1], reverse=1)

    # определяем количество событий
    print("List of words")
    for key, item in sort_list:
        count += 1
        # выдаем топ 30 популярных запросов
        if count < 30:
            print(key, "=>", item)
        else:
            break


