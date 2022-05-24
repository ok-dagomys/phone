import cfg
import shutil
import pandas
import filecmp
import os.path


async def phone_dictionary(greet):
    try:
        if not os.path.exists(cfg.dst) or not filecmp.cmp(cfg.src, cfg.dst):
            shutil.copy2(cfg.src, cfg.dst, follow_symlinks=False)
            filecmp.clear_cache()

            xl = pandas.ExcelFile(cfg.dst)

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

            df.to_sql(con=cfg.connection, name='dictionary', if_exists='replace')

            await cfg.bot.send_message(cfg.bot_id, greet)
            await cfg.asyncio.sleep(1)

    except Exception as ex:
        print(f'({cfg.date} {cfg.time}) Dictionary-Error: {ex}')