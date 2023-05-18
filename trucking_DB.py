import mysql.connector
from mysql.connector import Error
from datetime import datetime

bd = 'trucking'  # созданная БД

while True:  # циклическое выполнение действий
    print("""Меню действий:
                1 - ПОДКЛЮЧЕНИЕ, 2- ВВОД ЗАПРОСА, 3 - ДАННЫЕ ВОДИТЕЛЕЙ,
                4 - ДАННЫЕ ПЕРЕВОЗОК, 5 - ДАННЫЕ МАРШРУТОВ, 6 - ВВОД ЗАПИСИ ВОДИТЕЛЯ,
                7 - ВВОД ЗАПИСИ МАРШРУТА, 8 - ВВОД ЗАПИСИ ПЕРЕВОЗКИ, 9 - НАЗАНАЧЕНИЕ ВОДИТЕЛЯ НА ПЕРЕВОЗКУ,
                10 - ВВОДИТЕЛИ С САМЫМИ ДЛИТЕЛЬНЫМИ МАРШРУТАМИ, 11 - ВЫВОД МАРШРУТОВ В ДИАПАЗОНЕ,
                0 - ВЫХОД""")
    Code = int(input('Выберите действие: '))

    if Code == 0:  # завершение сеанса работы с БД
        try:
            cnx.close()
            print('сеанс работы с БД завершен')
        except Error as err:
            print(err)
        finally:
            break

    elif Code == 1:  # подключение к БД и создание курсора
        try:
            cnx = mysql.connector.connect(
                host="localhost", user='root', database=bd, password='89181024524Ni@')
            cursor1 = cnx.cursor()
            print('подключение к БД выполнено')
        except Error as e:
            print(e)

    elif Code == 2:  # выполнение произвольного запроса на выборку данных
        Query1 = input('Введите запрос на выборку данных: ')
        try:
            cnx.start_transaction()
            cursor1.execute(Query1)
            for row in cursor1.fetchall():
                print(row)
            cnx.commit()
        except Error as err:
            print(err)

    elif Code == 3:  # отображение данных водителей
        QueryDriver = "SELECT * FROM driversdata;"
        try:
            cursor1.execute(QueryDriver)
            for (Driver_ID, FirstName, LastName, Experience, Rate) in cursor1:
                print('{} - {} {}. Стаж - {} лет. Доплата за стаж - {} руб.'.format(Driver_ID, FirstName, LastName, Experience, Rate))
        except Error as err:
            print(err)

    elif Code == 4:  # отображение данных о перевозках
        QueryTransportations = "SELECT * FROM transportationsdate;"
        try:
            cursor1.execute(QueryTransportations)
            for (Transportation_ID, RouteName, Distance, EndDate, StartDate, FirstName, DriverPayment, LastName) in cursor1:
                print(
                    '{} - {}. Расстояние - {}км. Начало перевозки -{}. Начало перевозки - {}. ФИО водителя - {} {}'.format(Transportation_ID, RouteName, Distance,  StartDate, EndDate, LastName, FirstName))
        except Error as err:
            print(err)

    elif Code == 5:  # отображение данных о маршрутах
        QueryRoutes = "SELECT * FROM Routes"
        try:
            cursor1.execute(QueryRoutes)
            for (Route_ID, RouteName, Distance, DriverPayment) in cursor1:
                print('{} - {}. Расстояние - {}км. Оплата за маршрут - {}'.format(Route_ID, RouteName, Distance, DriverPayment))
        except Error as err:
            print(err)

    elif Code == 6:  # ввод данных о водителе
        print('введите данные о водителе')
        FirstName = input('Имя: ')
        LastName = input('Фамилия: ')
        Experience_ID = input('Опыт: ')
        QueryInputDriver = """INSERT INTO Drivers (Experience_ID, FirstName, LastName) VALUE ('{}','{}','{}')""".format(Experience_ID, FirstName, LastName)

        try:
            cursor1.execute(QueryInputDriver)
            cnx.commit()
            print('данные введены')
        except Error as err:
            print(err)

    elif Code == 7:  # ввод данных о маршруте
        print('введите данные о маршруте')
        StartCity = input('Город отправления: ')
        EndCity = input('Город назначения: ')
        RouteName = StartCity+'-'+EndCity
        Distance = input('Расстояние: ')
        QueryInputRoute = """INSERT INTO Routes (RouteName, Distance) VALUE ('{}', '{}')""".format(RouteName, Distance)

        try:
            cursor1.execute(QueryInputRoute)
            cnx.commit()
            print('данные введены')
        except Error as err:
            print(err)

    elif Code == 8:  # ввод данных о перевозке
        print('введите данные о перевозке')
        Route_ID = input('Код маршрута: ')
        print("Формат дат: ГГГГ-MM-ДД ЧЧ-ММ-СС")
        StartDate = input('Дата начала: ')
        EndDate = input('Дата конца: ')
        BonusPayment = input('Бонус за перевозку: ')
        QueryInputTransportation = """INSERT INTO transportations (Route_ID, EndDate, StartDate, BonusPayment) VALUE ('{}', '{}', '{}', '{}')""".format(Route_ID, EndDate, StartDate, BonusPayment)

        try:
            cursor1.execute(QueryInputTransportation)
            cnx.commit()
            print('данные введены')
        except Error as err:
            print(err)

    elif Code == 9:  # назначение водителя
        print('введите данные для назначения водителя')
        Route_ID = input('Код маршрута: ')
        Driver_ID = input('Код водителя: ')
        QueryInputTransportationDriver = """INSERT INTO transportationdrivers (Driver_ID, Route_ID) VALUE ('{}', '{}')""".format(Driver_ID, Route_ID)

        try:
            cursor1.execute(QueryInputTransportationDriver)
            cnx.commit()
            print('данные введены')
        except Error as err:
            print(err)

    elif Code == 10:  # вывод 3х самых длительных маршрутов

        try:
            cursor1.callproc('GetTopDriversWithLongestTrips')
            for result in cursor1.stored_results():
                for row in result.fetchall():
                    print(row)
        except Error as err:
            print(err)

    elif Code == 11:  # ввод смаршрутов в диапозоне по датам
        print('Фомат даты: ГГГГ-ММ-ДД')
        StartDate =(input('Введите 1-ю дату: '))
        EndDate =(input('Введите 2-ю дату: '))
        try:
            cursor1.callproc('GetRoutesInRange', [StartDate, EndDate])
            for result in cursor1.stored_results():
                for row in result.fetchall():
                    print(row)
        except Error as err:
            print(err)

print('работа программы завершена')
