from flask import Flask, jsonify, request
from app_server.db_scripts.cls_db import ClsDb
import json
from flask.sessions import SecureCookieSession

app = Flask(__name__) # запускем приложение как одиночный модуль
# Для реализации защиты от атак, основанных на подделке межсайтовых запросов
# (Cross-Site Request Forgery, CSRF), Flask-WTF требует от приложения
# настройки ключа шифрования. С помощью этого ключа Flask-WTF генерирует
# шифровальные блоки, которые используются для проверки аутентичности
# запросов, содержащие данные форм:

app.config['SECRET_KEY'] = 'Nbkm_Kbyltvfyy22'
sess = SecureCookieSession() # используем сессию для работы с user_id
db_conn = ClsDb()

@app.route('/', methods=['GET','POST'])
def index():
    return ('Это приложение для хранения и анализа содержания веществ в железорудном концентрате.')


@app.route('/api/login', methods=['GET','POST'])
def login():
    """
    Функция входа в приложение посредством логина и пароля
    Параметры из строки запроса (в формате JSON):
    - login
    - password
    :return:
    Если пользователь найден - возвращает его user_id,
    записывая его при этом в список данных сесии.
    Если пользователь не найден - возвращает словарь
    с сообщением об ошибке.
    """
    args = request.get_json()  # получаем параметры в формате JSON

    if 'login' not in args and 'password' not in args:
        return jsonify({'error':'Не задан логин или пароль'})
    else:
        lg = args['login']
        ps = args['password']

        resp = db_conn.login(lg, ps) # отправляем запрос в БД
        result = json.loads(resp) # получаем словарь из ответа на запрос

        if result != {'':''}:
            # пользователь найден - возвращаем результат
            user_id_found = result['idUsers']['0']
            sess['user_id'] = user_id_found # сохраняем для дальнейшего использования
            if not sess.modified:
                sess.modified = True
            return jsonify(user_id_found)
        else:
            # пользователь не найден
            return {'error':'Не привильно введены логин или пароль'}

@app.route('/api/insert_lab_data', methods=['GET','POST'])
def insert_lab_data():
    """
    Внесение данных в БД: качественных показателей железорудного концентрата
    Параметры на входе получаются из аргумента в строке URL и данных сессии.
    :return:
    """
    if 'user_id' in sess:
        args = request.get_json()  # получаем параметры в формате JSON
        usr_id = sess['user_id'] # получаем идентификатор пользователя

        result = db_conn.insert_into(user_id=usr_id, dic_of_lab=args) # запускаем вставку данных в БД
        return jsonify(result)
    else:
        result = {'error': 'Авторизируйтесь для получения доступа к этому разделу.'}
        return jsonify(result)

@app.route('/api/report', methods=['GET','POST'])
def report():
    """
    Получение данных для отчёта
    :return:
    """
    if 'user_id' in sess:
        args = request.get_json()  # получаем параметры в формате JSON

        month = args['month']
        user_id = sess['user_id']

        result = db_conn.get_info_for_report(month=month, user_id=user_id)
        return jsonify(result)
    else:
        result = {'error': 'Авторизируйтесь для получения доступа к этому разделу.'}
        return jsonify(result)

@app.route('/api/display', methods=["GET", 'POST'])
def display():
    """
    Вывод всех записей в БД из лаб. информации для текущего пользователя
    :return:
    """
    if 'user_id' in sess:
        user_id = sess['user_id']

        result = db_conn.get_info_for_report(user_id=user_id)
        return jsonify(result)
    else:
        result = {'error': 'Авторизируйтесь для получения доступа к этому разделу.'}
        return jsonify(result)

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')