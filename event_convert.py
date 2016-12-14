#!/usr/bin/python
# -*- coding: utf-8 -*-
# Python 2.7.5

import sys
from pymongo import MongoClient
from bson.objectid import ObjectId
from collections import OrderedDict


def convert(event0):
	# берем из оригинальной коллекции три поля
	_id = event0['_id']
	event = event0['events']['event'][0]
	# проверяем существование полей
	if 'description' in event:
		desc = event['description']
	else:
		desc = None

	typeid = event['typeId']
	# собираем первую часть объекта
	data0 = [('_id', ObjectId(_id)), ('description', desc), ('typeId', typeid)]

	# собираем вторую часть объекта из развернутого списка
	data1 = []
	if 'message' in event:
		s = event['message']['parameters']['parameter']
		for j, item2 in enumerate(s):
			if names[j] == item2['name']:
				data1.append([names[j], item2['value']])
		# сохраняем объект в упорядоченный словарь (нам важен порядок полей)
		data = OrderedDict(data0 + data1 + data2)
		result = db[user_new_coll].insert(data)
		return result
	else:
		return None

if __name__ == '__main__':

	# проверка аргументов
	if len(sys.argv) < 3:
		print("""Missing ip address console param. Examples:
		python event_convert.py 172.29.10.19 datasets test test_obf""")
		sys.exit(0)

	user_host = sys.argv[1]
	user_db = sys.argv[2]
	user_collection = sys.argv[3]
	user_new_coll = sys.argv[4]

	# хост MongoDB
	host = user_host
	client = MongoClient[host]
	db = client[user_db]
	coll = db[user_collection]

	lenght = db[user_collection].count({})
	count = 0
	events = []

	# список полей для конвертации
	names = [u'regNum', u'Address']
	# третья часть объекта (статическая)
	data2 = [('uploadedDate', None), ('userId', None), ('sessionId', None)]

	# выбираем из каждого события только необходимые поля
	# сборка списка из словарей
	events = []
	doc = []

	print ("Data export starts.\n")
	# составляем список событий эвакуации для конвертации
	for i, item in enumerate(db[user_collection].find({}, {"events":1})):
		events.append(item)

		# пакетный режим обработки данных
		if i % 5000 == 0:
			for item1 in events:
				count += 1
				convert(item1)

			# показываем процент выполненияa
			if i % 20000 == 0 and i != 0:
				print (i * 100 / lenght), "% done"
			# очищаем список с событиями
			events = []

		elif i + 1 == lenght:
			print "100% done"
			for item1 in events:
				count += 1
				convert(item1)

	print ("Data convertation complete.\n")

	# сообщаем о количестве событий и вставок в базу
	print ("Data import complete.\n" +
		   "There is", count, "converted field from", lenght)