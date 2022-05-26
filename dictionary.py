import asyncio
import filecmp
import os
import shutil
from datetime import datetime

import pandas
from dotenv import load_dotenv

load_dotenv()
dst = os.getenv('DST')
src = os.getenv('SRC')
date = datetime.now().strftime('%Y-%m-%d')
time = datetime.now().strftime('%H:%M:%S')


async def dictionary_request():
    try:
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copy2(src, dst, follow_symlinks=False)
            filecmp.clear_cache()

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

            await asyncio.sleep(1)
            return df

    except Exception as ex:
        print(f'({date} {time}) Dictionary-Error: {ex}')


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(dictionary_request())
