import mysql.connector
from datetime import date

def execute_query(connection, query, params=None):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query, params)
    result = cursor.fetchall()
    cursor.close()
    return result

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='ваш_хост',
            port=ваш_порт,
            database='ваша_бд',
            user='ваш_пользователь',
            password='ваш_пароль',
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Задание 1: Для указанной даты 1-ой рассылки вывести список приглашенных и посчитать их количество
def list_invited_and_count(connection, first_invitation_date):
    query = '''
        SELECT *
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
        WHERE cpn.FirstInvitationDate = %s
    '''
    params = (first_invitation_date,)
    result = execute_query(connection, query, params)
    print("Список приглашенных:")
    print(result)

    count_query = '''
        SELECT COUNT(*)
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
        WHERE cpn.FirstInvitationDate = %s
    '''
    count_result = execute_query(connection, count_query, params)
    print("Количество приглашенных:")
    print(count_result[0]['COUNT(*)'])

# Задание 2: Добавление приглашенных на конференцию с указанием оргвзноса и даты его уплаты
def add_participant_with_payment(connection, last_name, first_name, middle_name, payment_date, payment_amount):
    query = '''
        INSERT INTO ConferenceParticipants (LastName, FirstName, MiddleName)
        VALUES (%s, %s, %s)
    '''
    params = (last_name, first_name, middle_name)
    execute_query(connection, query, params)

    participant_id_query = '''
        SELECT ParticipantID
        FROM ConferenceParticipants
        WHERE LastName = %s AND FirstName = %s AND MiddleName = %s
    '''
    participant_id_params = (last_name, first_name, middle_name)
    participant_id_result = execute_query(connection, participant_id_query, participant_id_params)
    participant_id = participant_id_result[0]['ParticipantID']

    payment_query = '''
        INSERT INTO ConferenceParticipation (ParticipantID, PaymentSubmissionDate, PaymentAmount)
        VALUES (%s, %s, %s)
    '''
    payment_params = (participant_id, payment_date, payment_amount)
    execute_query(connection, payment_query, payment_params)

# Задание 3: Вывести список приглашенных, с указанием даты об уплате оргвзноса
def list_participants_with_payment_date(connection):
    query = '''
        SELECT cp.LastName, cp.FirstName, cp.MiddleName, cpn.PaymentSubmissionDate
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
    '''
    result = execute_query(connection, query)
    print("Список приглашенных с указанием даты об уплате оргвзноса:")
    print(result)

# Задание 4: Вывести список участников, уплативших оргвзнос в указанном диапазоне дат
def list_paid_participants_in_date_range(connection, start_date, end_date):
    query = '''
        SELECT cp.LastName, cp.FirstName, cp.MiddleName, cpn.PaymentSubmissionDate
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
        WHERE cpn.PaymentSubmissionDate BETWEEN %s AND %s
    '''
    params = (start_date, end_date)
    result = execute_query(connection, query, params)
    print("Список участников, уплативших оргвзнос в указанном диапазоне дат:")
    print(result)

# Задание 5: Вывести название тезисов докладов, поступивших из указанного города
def theses_from_city(connection, city):
    query = '''
        SELECT cp.City, cpn.PresentationTopic
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
        WHERE cp.City = %s
    '''
    params = (city,)
    result = execute_query(connection, query, params)
    print(f"Название тезисов докладов, поступивших из города {city}:")
    print(result)

# Задание 6: Вывести список нуждающихся в гостинице для указанного города
def hotel_needs_in_city(connection, city):
    query = '''
        SELECT cp.LastName, cp.FirstName, cp.MiddleName, cpn.ArrivalDate, cpn.DepartureDate
        FROM ConferenceParticipants cp
        JOIN ConferenceParticipation cpn ON cp.ParticipantID = cpn.ParticipantID
        WHERE cp.City = %s AND cpn.HotelNeed = 1
    '''
    params = (city,)
    result = execute_query(connection, query, params)
    print(f"Список нуждающихся в гостинице из города {city}:")
    print(result)

# Подключаемся к базе данных
connection = connect_to_database()

if not connection:
    print("Не удалось подключиться к базе данных.")
else:
    try:
        # Задание 1
        first_invitation_date = date(2023, 1, 15)
        list_invited_and_count(connection, first_invitation_date)

        # Задание 2
        add_participant_with_payment(connection, "Иванов", "Иван", "Иванович", date(2023, 1, 20), 500.0)

        # Задание 3
        list_participants_with_payment_date(connection)

        # Задание 4
        start_date = date(2023, 1, 1)
        end_date = date(2023, 2, 1)
        list_paid_participants_in_date_range(connection, start_date, end_date)

        # Задание 5
        theses_from_city(connection, "Москва")

        # Задание 6
        hotel_needs_in_city(connection, "Санкт-Петербург")

    except Exception as e:
        print(f"Error: {e}")

    # Закрываем соединение с базой данных
    connection.close()
