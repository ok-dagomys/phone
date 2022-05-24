import cfg


def caller_recognition(caller, number):
    try:
        recognition = cfg.connection.execute(cfg.text(
            f'SELECT * FROM dictionary WHERE dictionary.number LIKE "%{caller}%";'
        )).fetchone()  # Для поиска всех совпадений необходимо использовать .fetchall()
        if recognition:
            return f'Входящий звонок\nс номера: {caller}\nна номер: {number}' \
                   f'\nот: {recognition[3]}\n{recognition[1]} {recognition[2]}\n'
        else:
            return f'Входящий звонок\nс номера: {caller}\nна номер: {number}'

    except Exception as ex:
        print(f'({cfg.date} {cfg.time}) Caller_recognition-Error: {ex}')


def name_recognition(message):
    try:
        cfg.search_id = message.message_id
        cfg.user_id = message.from_user.id
        search_name = message.text.split()
        name = search_name[1].strip().lower().title()
        recognition = cfg.connection.execute(cfg.text(
            f'SELECT * FROM dictionary WHERE dictionary.name LIKE "%{name}%";'
        )).fetchone()  # Для поиска всех совпадений необходимо использовать .fetchall()

        return f'ФИО: {recognition[1]} {recognition[2]}\n' \
               f'Должность: {recognition[3]}\n'\
               f'Отдел: {recognition[4]}\n'\
               f'Внутренний: {recognition[5]}\n'\
               f'Рабочий: {recognition[6]}\n'\
               f'Мобильный: {recognition[7]}\n'

    except Exception as ex:
        print(f'({cfg.date} {cfg.time}) Name_recognition-Error: {ex}')
