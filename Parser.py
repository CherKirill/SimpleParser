import psycopg2
import vk_api
import datetime
from psycopg2 import Error

#токен для доступа к апи вк
key ='05d78f8505d78f8505d78f85aa06c132cc005d705d78f85605d5311d02ab5f2e64cd9ee'

#название группы откуда будем брать новости
domain = 'overhear_tpu'
#domain ='tpunews'

#параметры для подключения к удаленной бд
user = "zexccsqi"
host = "tiny.db.elephantsql.com"
password = "87Ts-zNNXQ67YiJoEzQB7-aS2IahauWL"
database = "tiny.db.elephantsql.com"

#словарь для сверки уже с уже спарсенными сообщениями
dict = dict()

try:
    #подключение к бд
    connection = psycopg2.connect(user=user,
                              password = password,
                              dbname = "zexccsqi",
                              host = host,
                              port ="5432")

    cursor = connection.cursor()

    #создание запроса, для взятия данных с бд о уже существующих постах
    postgresql_select_query = "select code, owner_id from post"

    cursor.execute(postgresql_select_query)
    mobile_records = cursor.fetchall()


    for row in mobile_records:
        dict[row[0]] = row[1] #заполнение словаря хешами постов
    vk = vk_api.VkApi(token=key)
    api = vk.get_api()
    domain = domain

    totatCount = api.wall.get(domain=domain)['count']
    count = 100 #каждые 100 постов будем брать с апи
    offset = 0 #текущий обработанный шаг

    while offset<totatCount:
        #забираем посты из массива items
        text = api.wall.get(domain=domain, count=count, offset = offset)['items']

        i = 0
        while i < count:
            #забираем hash текущего поста
            hash = text[i]['hash']
            
            #если пост раннее не обрабатывался
            if(hash not in dict):
                #то начинаем обрабодку
                try:
                    #подготовливаем запрос в бд для вставки данных
                    postgresql_select_query = "INSERT INTO post (code, text, owner_id, comments_count, date) VALUES (%s, %s, %s, %s, %s);"

                    post = text[i]['text'] #текст поста
                    post_id = text[i]['id'] #id поста
                    owner_id = text[i]['owner_id'] #id владельца поста
                    date = text[i]['date'] #дата поста

                    countComments = text[i]['comments']['count'] #колличество комментариев под постом (всё дерево)

                    try:
                        datetime_obj = datetime.datetime.fromtimestamp(date) #перевод числа даты в полноценный формат
                    except (Exception, Error) as error:
                        print("Ошибка в обработке времени", error)

                    #применяем запрос
                    cursor.execute(postgresql_select_query, (hash, post, owner_id, countComments, str(datetime_obj)))
                    connection.commit()

                    #если колличество комментариев привышает 0
                    if(countComments > 0):
                        #то объявляем счётчик
                        comCount = 0
                        #забираем максимальное колличество комментариев первого уровня
                        comments = api.wall.getComments(owner_id = owner_id, post_id = post_id, count = countComments)
                        #узнаем их колличество
                        lenItems = len(comments["items"])
                        while comCount < lenItems:
                                try:
                                    #подготовливаем запрос в бд для вставки данных
                                    postgresql_select_query = "INSERT INTO comments_post (comment_id, text, post_id, date) VALUES (%s, %s, %s, %s);"

                                    post = comments["items"][comCount]['text']#текст комментария
                                    id = comments["items"][comCount]['id']#id комментария
                                    date = comments["items"][comCount]['date']#дата комментария

                                    try:
                                        datetime_obj = datetime.datetime.fromtimestamp(date)#перевод числа даты в полноценный формат
                                    except (Exception, Error) as error:
                                        print("Ошибка в обработке времени", error)

                                    #применяем запрос
                                    cursor.execute(postgresql_select_query, (id, post, post_id, str(datetime_obj)))
                                    connection.commit()
                                    comCount+=1
                                except (Exception, Error) as error:
                                    print("Ошибка при работе обработке запроса", error)
                except (Exception, Error) as error:
                    print("Ошибка при работе обработке запроса", error)

            i+=1
        offset += count


except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")