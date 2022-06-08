import asyncio
import filecmp
import logging
import os
import shutil
from datetime import datetime

import pandas
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
dst = os.getenv('DST')
src = os.getenv('SRC')
date = datetime.now().strftime('%Y-%m-%d')
time = datetime.now().strftime('%H:%M:%S')
logging.basicConfig(level=logging.INFO)


async def check_phonebook():
    if not os.path.exists(dst) or not filecmp.cmp(src, dst):
        logging.info('Phonebook has a new version on server')

        shutil.copy2(src, dst, follow_symlinks=False)
        with tqdm(total=100) as pbar:
            for i in range(10):
                await asyncio.sleep(0.1)
                pbar.update(10)
                pbar.set_description("Copying...")
        filecmp.clear_cache()

        status = 'Phonebook updated'
        logging.info(status)
    else:
        status = 'Phonebook is actual'
        logging.info(status)
    return status


async def filter_phonebook():
    await check_phonebook()

    xl = pandas.ExcelFile(dst)

    sheets = xl.sheet_names
    sheets.remove('Тетьково')

    df = pandas.DataFrame()
    rem_words = ['Столбец1', 'Столбец2', 'Столбец3', 'Столбец4', 'Столбец5', 'Столбец6', 'Столбец7',
                 'Фамилия', 'Имя Отчество', 'Должность', 'Отдел (служба)', 'СО № 7',
                 'Внутренний телефон', 'Городской телефон', 'Мобильный телефон']
    for sheet in sheets:
        data = pandas.read_excel(xl, sheet_name=sheet, header=None, index_col=None)
        # df = df.append(data)
        df = pandas.concat([df, data])
    for word in rem_words:
        df = df.mask(df == word)
    xl.close()

    df = df[[1, 2, 3, 4, 5, 6, 7]].dropna(axis=0, how='all')
    df = df[[1, 2, 3, 4, 5, 6, 7]].fillna('', axis=1)
    df.columns = ['name', 'surname', 'role', 'department', 'number', 'work', 'mobile']
    df = df.replace('-', '', regex=True).astype(str).reset_index(drop=True)
    df = df.drop(df[(df['number'] == '') & (df['work'] == '') & (df['mobile'] == '')]
                 .index, axis=0).reset_index(drop=True)

    def column_filter(column_name):
        if column_name in ['number', 'work', 'mobile']:
            df[column_name] = df[column_name].apply(lambda x: x.replace('(факс)', '').split())
            df[column_name] = df[column_name].apply(lambda x: ','.join(map(str, x)).replace(',', ', '))
        else:
            df[column_name] = df[column_name].apply(lambda x: x.split())
            df[column_name] = df[column_name].apply(lambda x: ' '.join(map(str, x)).replace('"', "'"))

    for name in df.columns:
        column_filter(name)

    await asyncio.sleep(0.1)
    return df


async def name_recognition(text):
    dictionary = await filter_phonebook()
    search_name = text.split()
    name = search_name[1].strip().lower().title()
    recognition = dictionary[dictionary['name'].str.contains(name)]
    if recognition.shape[0] > 0:
        return f'ФИО: {recognition.iloc[0]["name"]} {recognition.iloc[0]["surname"]}\n'\
               f'Должность: {recognition.iloc[0]["role"]}\n'\
               f'Отдел: {recognition.iloc[0]["department"]}\n'\
               f'Внутренний: {recognition.iloc[0]["number"]}\n'\
               f'Рабочий: {recognition.iloc[0]["work"]}\n'\
               f'Мобильный: {recognition.iloc[0]["mobile"]}\n'
    else:
        return f'Совпадений по запросу "{name}" не найдено'


async def caller_recognition(caller, number):
    dictionary = await filter_phonebook()
    recognition = dictionary[dictionary['number'].str.contains(caller)]
    if recognition.shape[0] > 0:
        return f'Входящий звонок\nс номера: {caller}\nна номер: {number}' \
               f'\nот: {recognition.iloc[0]["role"]}\n{recognition.iloc[0]["name"]} {recognition.iloc[0]["surname"]}\n'
    else:
        return f'Входящий звонок\nс номера: {caller}\nна номер: {number}'


async def start():
    await check_phonebook()
    await asyncio.sleep(0.1)

asyncio.run(start())
