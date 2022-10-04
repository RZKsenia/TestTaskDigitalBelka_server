import pandas as pd
import pymysql
import numpy as np

class ClsDb(object):
    dialect= 'MYSQL'
    driver = 'MySQL-Python'
    username = 'root'
    password = 'Ybrnjrhjvtyfc_2022'
    host = '172.17.0.2'
    port = 3306
    database = 'TestDB'

    def __init__(self):
        # Создаём соединение с БД, работающей в контейнере docker:
        self.connection = pymysql.connect(host= self.host,
                                          port= self.port,
                                          user= self.username,
                                          password= self.password,
                                          database= self.database,
                                          cursorclass= pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()

    def login(self, lg, ps):
        """
        Проверка - есть ли пользователь с указанным логином и паролем
        в базе данных. Если есть - возвращаем ID пользователя, если нет -
        возвращаем None.
        :param lg: имя пользователя (логин)
        :param ps: пароль
        :return:
        """
        lg = chr(34) + lg + chr(34)
        ps = chr(34) + ps + chr(34)
        sql = 'SELECT * FROM Users WHERE user_name=%s and user_password=%s;' % (lg, ps)
        self.cursor.execute(sql)
        result = pd.DataFrame(self.cursor.fetchall())

        if len(result.index) == 0:
            # не найден пользователь с такими именем и паролем
            return '{"":""}'
        else:
            return result.to_json() # ответ - текстовая строка в формате JSON, т.е. '{a:b}'

    def insert_into(self, user_id, dic_of_lab):
        """
        Вставка данных в таблицу лабораторных данных о
        содержании веществ в железорудном концентрате
        :param dic_of_lab: словарь, содержащий параметры - содержание веществ
        в железорудном концентрате
        :return:
        """
        concentrat_title = dic_of_lab['concentrat_title'] # наименование концентрата
        concentrat_title = chr(34) + concentrat_title + chr(34)

        month = dic_of_lab['month']  # месяц внесения данных
        month = chr(34) + month + chr(34)

        fer = dic_of_lab['fer']  # содержание железа
        crmn = dic_of_lab['crmn']  # содержание кремния
        allum = dic_of_lab['allum']  # содержание аллюминия
        clm = dic_of_lab['clm']  # содержание кальция
        sr = dic_of_lab['sr']  # содержание серы

        sql_str = 'INSERT INTO lab_info VALUES (Null, %s, %s, %s, %s, %s, %s, %s, %s);' % \
                  (user_id, concentrat_title, month, fer, crmn, allum, clm, sr)

        self.cursor.execute(sql_str) # выполняем запрос на добавление данных в таблицу
        self.connection.commit() # применяем внесённые а БД изменения

        sql_str = 'SELECT * FROM lab_info;'
        self.cursor.execute(sql_str) # выполняем запрос на получение всех данных из таблицы lab_info
        result = pd.DataFrame(self.cursor.fetchall()) # результат запроса записываем в DataFrame

        return result.to_json() # ответ - текстовая строка в формате JSON, т.е. '{a:b}'

    def get_info_for_report(self, user_id, month=''):
        """
        В зависимости от полученных на вход аргументов:
        user_id и month: возвращает статистические данные (min, max, mean) по записям из таблицы lab_info
        для текущиего авторизированного пользователя (user_id) за указанный месяц (month).
        u
        ser_id: возвращает все введённые авторизированным пользователем (user_id) в таблицу lab_info
        данные без учёта месяца.

        :param month: месяц, за который нужны данные. По умолчанию равен пустой строке.
        :param user_id: идентификатор пользователя для фильтрации данных в БД
        :return:
        """
        if month != '':
            sql_str = """SELECT
                         concentrat_title as 'Концентрат',
                         month_title as 'Месяц',
                         ferum as 'Железо',
                         cremnium as 'Кремний',
                         aluminium as 'Алюминий',
                         calcium as 'Кальций',
                         sera as 'Сера'
                         FROM lab_info
                         WHERE month_title=%s AND user_id=%s
                         ORDER BY month_title, 
                         concentrat_title;""" % (month, user_id)
            self.cursor.execute(sql_str)
            pre_result = pd.DataFrame(self.cursor.fetchall()) # загружаем результат запроса в DataFrame
            result = pre_result.groupby(['Концентрат']) # группируем по Концентрату
            result = result.agg([np.mean, np.min, np.max]) # вычисляем аггрегирующие функции
        else:
            sql_str = """SELECT
                         concentrat_title as 'Концентрат',
                         month_title as 'Месяц',
                         ferum as 'Железо',
                         cremnium as 'Кремний',
                         aluminium as 'Алюминий',
                         calcium as 'Кальций',
                         sera as 'Сера'
                         FROM lab_info
                         WHERE user_id=%s
                         ORDER BY month_title, 
                         concentrat_title;""" % user_id
            self.cursor.execute(sql_str)
            result = pd.DataFrame(self.cursor.fetchall())

        return result.to_json()  # ответ - текстовая строка в формате JSON, т.е. '{a:b}'