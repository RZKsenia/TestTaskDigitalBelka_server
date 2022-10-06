import pandas as pd
import pymysql
import numpy as np

class ClsDb(object):
    dialect = 'MYSQL'
    driver = 'MySQL-Python'
    username = 'root'
    password = 'Ybrnjrhjvtyfc_2022'
    host = '172.17.0.2'
    port = 3306
    database = 'TestDB'

    def get_new_connection_to_db(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.username,
                               password=self.password,
                               database=self.database,
                               cursorclass=pymysql.cursors.DictCursor)

    def login(self, lg, ps):
        """
        Проверка - есть ли пользователь с указанным логином и паролем
        в базе данных. Если есть - возвращаем ID пользователя, если нет -
        возвращаем None.
        :param lg: имя пользователя (логин)
        :param ps: пароль
        :return:
        """
        conn = self.get_new_connection_to_db()
        cur = conn.cursor()
        lg = chr(34) + lg + chr(34)
        ps = chr(34) + ps + chr(34)
        sql = 'SELECT * FROM Users WHERE user_name=%s and user_password=%s;' % (lg, ps)
        cur.execute(sql)
        result = pd.DataFrame(cur.fetchall())
        cur.close()
        conn.close()

        if len(result.index) == 0:
            # не найден пользователь с такими именем и паролем
            return '{"":""}'
        else:
            return result.to_json()  # ответ - текстовая строка в формате JSON, т.е. '{a:b}'

    def insert_into(self, user_id, dic_of_lab):
        """
        Вставка данных в таблицу лабораторных данных о
        содержании веществ в железорудном концентрате
        :param dic_of_lab: словарь, содержащий параметры - содержание веществ
        в железорудном концентрате
        :return:
        """
        conn = self.get_new_connection_to_db()
        cur = conn.cursor()
        concentrat_title = dic_of_lab['concentrat_title']  # наименование концентрата
        concentrat_title = chr(34) + concentrat_title + chr(34)

        month = dic_of_lab['month']  # месяц внесения данных
        month = chr(34) + month + chr(34)

        fer = dic_of_lab['fer'].replace(',', '.')  # содержание железа
        crmn = dic_of_lab['crmn'].replace(',', '.')  # содержание кремния
        alum = dic_of_lab['alum'].replace(',', '.')  # содержание аллюминия
        clm = dic_of_lab['clm'].replace(',', '.')  # содержание кальция
        sr = dic_of_lab['sr'].replace(',', '.')  # содержание серы

        sql_str = """INSERT INTO lab_info VALUES (Null, %s, %s, %s, %s, %s, %s, %s, %s);""" % \
                  (user_id, concentrat_title, month, fer, crmn, alum, clm, sr)

        response = cur.execute(sql_str)  # выполняем запрос на получение всех данных из таблицы lab_info
        conn.commit()
        cur.close()
        conn.close()

        conn = self.get_new_connection_to_db()
        cur = conn.cursor()
        sql_new = 'SELECT * FROM lab_info;'
        response2 = cur.execute(sql_new)
        result = pd.DataFrame(cur.fetchall())  # результат запроса записываем в DataFrame
        cur.close()
        conn.close()

        return result.to_json()  # ответ - текстовая строка в формате JSON, т.е. '{a:b}'

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
                         ferum as 'Железо',
                         cremnium as 'Кремний',
                         aluminium as 'Алюминий',
                         calcium as 'Кальций',
                         sera as 'Сера'
                         FROM lab_info
                         WHERE month_title=%s AND user_id=%s                         
                         ORDER BY concentrat_title;""" % (month, user_id)
            conn = self.get_new_connection_to_db()
            cur = conn.cursor()
            cur.execute(sql_str)
            pre_result = pd.DataFrame(cur.fetchall())  # загружаем результат запроса в DataFrame
            cur.close()
            conn.close()

            if pre_result.shape[0] != 0:
                pre_result = pre_result.groupby(['Концентрат'])  # группируем по Концентрату
                pre_result = pre_result.agg([np.mean, np.min, np.max])  # вычисляем аггрегирующие функции
                result = pre_result.to_json()
            else:
                # если в БД ещё нет для авторизированного пользователя
                # никаких записей о содержании веществ в железорудном концентрате,
                # то выводим пустой Dataframe:
                dt_for_df = {'Концентрат': '',
                             'Месяц': '',
                             'Железо': '',
                             'Кремний': '',
                             'Алюминий': '',
                             'Кальций': '',
                             'Сера': ''
                             }
                pre_result = pd.DataFrame(dt_for_df, index=['0'])
                # преобразуем DataFrame в текстовую строку в формате,
                # соответствующем JSON:
                result = pre_result.to_json()
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
            conn = self.get_new_connection_to_db()
            cur = conn.cursor()
            cur.execute(sql_str)
            pre_result = pd.DataFrame(cur.fetchall())
            cur.close()
            conn.close()
            # преобразуем DataFrame в текстовую строку в формате,
            # соответствующем JSON:
            result = pre_result.to_json()
        # текстовая строка в формате,
        # соответствующем JSON:
        return result
