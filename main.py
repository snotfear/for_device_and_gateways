import psycopg2

con = psycopg2.connect(
  database="postgres",
  user="postgres",
  password="",
  host="127.0.0.1",
  port="5432"
)


def find_neighbors(latitude, longitute, cursor):
    '''Функция возвращает тсписок ID соседних устройств в радиусе 50м
    latitude, longitute - координаты устройства в состоянии offline,
    cursor - объект курсора для работы с базой'''

    lati = latitude
    longi = longitute
    koef = 0.1 #Перевод из меры измерений координат в метры. Добавить позже
    cursor.execute("SELECT id, online FROM device "
                   "WHERE (SQRT((POWER(latitude - %(lati)s),2) + POWER(longitute - %(longi)s),2),2) * %(koef)s) < 50",
                   {'lati': lati, 'longi': longi, 'koef': koef})
    list_device_id_online = cursor.fetchall()
    return list_device_id_online

def calculate_percent_offline(table):
    '''Рассчёт процента оффлайн устройств среди соседей
    table - тадица содержащая id и статус устройств'''

    sum_percent = 0
    for row in table:
        if row[1] == 'false':
            sum_percent += 1
    return round(sum_percent *100 /len(table))

def first_choise(percent, cursor, longitute, latitude, table_neighbors):
    '''Итоговая функция, выводящая результат проверки.
    percent - процент молчащих устройств
    table_neighbors - список соседних устройств (до 50м)'''

    if percent < 30:
        return 'инфраструктура в подрядке'
    elif percent == 100:
        nearest_gateway = find_nearest_gateway(cursor=cursor, longitute=longitute, latitude=latitude)
        if 'True' in nearest_gateway:
            return 'инфраструктура в подрядке'
    elif 30<=percent<100:
        sum_quality = 0
        for neighbor in table_neighbors:
            current_quality = qualities(device_id=neighbor[0], cursor=cursor)
            sum_quality += current_quality
        avg_quality = sum_quality/len(table_neighbors)
        if avg_quality != 0:
            return 'инфраструктура в подрядке'
        else:
            return 'Ошибка в инфраструктуре'

def find_nearest_gateway(cursor, latitude, longitute):
    '''функция возвращает список блжайших базовых станций в радиусе 3км,
    latitude, longitute - координаты устройства в состоянии offline,
    cursor - объект курсора для работы с базой'''

    lati = latitude
    longi = longitute
    koef = 0.1
    cursor.execute("SELECT active FROM gateways "
                   "WHERE (SQRT((POWER(latitude - %(lati)s),2) + POWER(longitude - %(longi)s),2),2) * %(koef)s) < 3000",
                   {'lati': lati, 'longi': longi, 'koef': koef})
    active_gateways_list = cursor.fetchall()
    return active_gateways_list


def qualities(device_id, cursor):
    '''находит список всех подключённых к устройству базовых станций и возвращает
    среднее качество связи для одного устройства'''

    cursor.execute('SELECT gate_id, signal_quality FROM gateways_quality'
                   'WHERE %(device_id)s = device_id', {'device_id': device_id}
                   )
    gateway_list = cursor.fetchall()
    sum_signal_quality = 0
    for row in gateway_list:
        sum_signal_quality += row[1]
    return sum_signal_quality/len(gateway_list)

cur = con.cursor()
cur.execute("SELECT id, latitude, longitute FROM device WHERE online = False")
table_offline = cur.fetchall()

'''Нашли все неработающие устройства,
для каждого девайса без ответа запускаем функции
итогом будет печать вывода'''

for each in table_offline:
    list_neighbors = find_neighbors(latitude=each[1], longitute=each[2], cursor=cur) #[id, online]
    current_percent = calculate_percent_offline(list_neighbors)
    print(first_choise(percent=current_percent, cursor=cur, latitude=each[1], longitute=each[2], table_neighbors=list_neighbors))
