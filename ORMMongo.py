import csv
import re
from datetime import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING

client = MongoClient()  # netstat -na    - активные подключения (для проверки работы MongoDB) - адрес 27017
netology_db = client['netology']
concerts_collection = netology_db['concerts_collection']

csv_file = 'artists.csv'
with open(csv_file, encoding='utf8') as csvfile:
    # прочитать файл с данными и записать в коллекцию
    reader = csv.DictReader(csvfile)
    new_concerts = []
    id = 0
    for row in reader:
        id += 1
        row = dict(row)
        split_data = re.split('\.', row['Дата'])
        data_for_db = datetime(year=2020, month=int(split_data[1]), day=int(split_data[0]))
        row['Дата'] = data_for_db
        row['_id'] = id
        row['Цена'] = int(row['Цена'])
        concerts_collection.update_one({'_id': id}, {"$set": row}, upsert=True)


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    for new in db.find().sort('Цена', ASCENDING):
        print(new)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    regex = re.compile(re.escape(name))
    # ('укажите регулярное выражение для поиска. ' \'Обратите внимание, что в строке могут быть специальные символы, их нужно экранировать')

    if (db.count_documents({'Исполнитель': regex}))>0:
        print('Найденные билеты по имени исполнителя по возрастанию цены:')
        for new in db.find({'Исполнитель': regex}).sort('Цена', ASCENDING):
            print(new)
    else:
        print('Исполнители не найдены')



if __name__ == '__main__':
    print("Функция - отсортировать билеты по возрастанию цены")
    find_cheapest(concerts_collection)
    print("Функция - поиск билеты по исполнителю по возрастанию цены")
    find_by_name('а', concerts_collection)
