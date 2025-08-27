import os
import json
import pandas as pd
import openpyxl
from sqlalchemy import create_engine
from glob import glob
from datetime import datetime


# Читаем файл cred, в котором хранятся данные для подключения
with open('cred.json', 'r', encoding='utf-8') as f:
    cred = json.load(f)

# Подключаемся к базе через sqlalchemy
url = f'postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['dbname']}'
engine = create_engine(url)


# Функция для загрузки данных из файлов passport_blacklist в sql с помощью sqlalchemy и pandas
def passport_blacklist_to_sql():
    """Эта функция обрабатывает файлы passport_blacklist*.xlsx,
    загружает данные в postgresql в таблицу stg_passport_blacklist
    и перемещает обработанный файл в папку archive"""

    os.chdir('Data')
    # Ищем все файлы passport_blacklist в папке Data и сортируем их по дате
    passport_blacklist_files = glob('passport_blacklist*')
    passport_blacklist_files.sort()
    # Читаем файлы
    for file in passport_blacklist_files:
        df = pd.read_excel(file)
        # Записываем данные в БД в таблицу STG_passport_blacklist
        df.to_sql('stg_passport_blacklist', con=engine, schema='bank',
                  if_exists='append', index=False)
        # Перемещаем и переименовываем обработанный файл
        os.replace(os.path.join(file),
                   os.path.join('archive', file + '.backup'))
    os.chdir('..')
    print('Функция passport_blacklist_to_sql успешно загрузила данные в таблицу stg_passport_blacklist')


# Функция для загрузки данных из файлов terminals в sql с помощью sqlalchemy и pandas
def terminals_to_sql():
    """Эта функция обрабатывает файлы terminals*.xlsx,
    загружает данные в postgresql в таблицу stg_terminals
    и перемещает обработанный файл в папку archive"""

    os.chdir('Data')
    # Ищем все файлы terminals в папке Data и сортируем их по дате
    terminals_files = glob('terminals*')
    terminals_files.sort()
    # Читаем название файла
    for file in terminals_files:
        # Вырезаем дату из названия файла
        date_str = file[file.rfind('_') + 1: -5]
        # Преобразуем в дату и создаем DataFrame
        date_file = datetime.strptime(date_str, '%d%m%Y').date()
        date_column = pd.DataFrame([{'effective_from': date_file}])
        # Читаем содержимое файла
        df = pd.read_excel(file)
        # Объединяем DataFrame из файла с датой
        df_concat = pd.concat([df, date_column], axis=1)
        # Заполняем пропуски в дате
        df_concat['effective_from'] = df_concat['effective_from'].fillna(
            date_file)
        # Записываем в БД
        df_concat.to_sql('stg_terminals', con=engine, schema='bank',
                         if_exists='append', index=False)
        # Перемещаем и переименовываем обработанный файл
        os.replace(os.path.join(file),
                   os.path.join('archive', file + '.backup'))
    os.chdir('..')
    print('Функция terminals_to_sql успешно загрузила данные в таблицу stg_terminals')

# Функция для загрузки данных из файлов transactions в sql с помощью sqlalchemy и pandas


def transactions_to_sql():
    """Эта функция обрабатывает файлы transactions*.txt,
    загружает данные в postgresql в таблицу stg_transactions
    и перемещает обработанный файл в папку archive"""

    os.chdir('Data')
    # Ищем все файлы transactions в папке Data и сортируем их по дате
    transactions_files = glob('transactions*')
    transactions_files.sort()
    # Читаем файлы
    for file in transactions_files:
        df = pd.read_csv(file, sep=';')
        # Записываем данные в БД в таблицу STG_transactions
        df.to_sql('stg_transactions', con=engine, schema='bank',
                  if_exists='append', index=False)
        # Перемещаем и переименовываем обработанный файл
        os.replace(os.path.join(file),
                   os.path.join('archive', file + '.backup'))
    os.chdir('..')
    print('Функция transactions_to_sql успешно загрузила данные в таблицу stg_transactions')
