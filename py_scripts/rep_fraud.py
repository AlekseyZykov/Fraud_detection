import psycopg2
import json


# Читаем файл cred, в котором хранятся данные для подключения
with open('cred.json', 'r', encoding='utf-8') as f:
    cred = json.load(f)


def v_invalid_or_blacklist_passport():
    """Функция создает представление v_invalid_or_blacklist_passport с операциями совершенными
    при просроченном или заблокированном паспорте"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute(
            """DROP VIEW IF EXISTS v_invalid_or_blacklist_passport;""")
        cursor.execute("""CREATE VIEW v_invalid_or_blacklist_passport as
    SELECT
        t5.transaction_date AS event_dt,
        t1.passport_num AS passport,
        concat_ws(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
        t1.phone AS phone,
        'invalid_or_blacklist_passport' AS event_type,
        CAST(current_timestamp AS timestamp)  AS report_dt
    from clients t1
    left JOIN dwh_fact_passport_blacklist t2
    ON t1.passport_num = t2.passport_num
    left JOIN accounts t3
    ON t1.client_id = t3.client
    left JOIN cards t4
    ON t3.account = t4.account
    left JOIN dwh_fact_transaction t5
    ON t4.card_num = t5.card_num
    -- Фильтруем просроченные паспорта и внесенные в blacklist до даты транзакции
    WHERE date_trunc('day', t5.transaction_date) > t2.entry_dt
    OR    date_trunc('day', t5.transaction_date) > t1.passport_valid_to;""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция v_invalid_or_blacklist_passport успешно загрузила данные в представление v_invalid_or_blacklist_passport')
    except Exception:
        connection.rollback()
        print('Функции v_invalid_or_blacklist_passport не удалось загрузить данные в представление v_invalid_or_blacklist_passport')


def v_invalid_account():
    """Функция создает представление v_invalid_account с операциями совершенными
    при недействующем договоре"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""DROP VIEW IF EXISTS v_invalid_account;""")
        cursor.execute("""CREATE VIEW v_invalid_account AS 
SELECT 
	t4.transaction_date AS event_dt,
	t1.passport_num AS passport,
	concat_ws(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
	t1.phone AS phone,
	'invalid_account' AS event_type,
	CAST(current_timestamp AS timestamp)  AS report_dt
from clients t1
left JOIN accounts t2
ON t1.client_id = t2.client
left JOIN cards t3
ON t2.account = t3.account
left JOIN dwh_fact_transaction t4
ON t3.card_num = t4.card_num
-- Фильтруем недействующие договоры
WHERE date_trunc('day', t4.transaction_date) > t2.valid_to""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция v_invalid_account успешно загрузила данные в представление v_invalid_account')
    except Exception:
        connection.rollback()
        print('Функции v_invalid_account не удалось загрузить данные в представление v_invalid_account')


def v_diff_town():
    """Функция создает представление v_diff_town с операциями совершенными
    одним клиентом в разных городах в течение одного часа"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""DROP VIEW IF EXISTS v_tmp_diff_town;""")
        # Временное представление
        cursor.execute("""CREATE VIEW v_tmp_diff_town AS 
SELECT 
	t4.transaction_date AS event_dt,
	t1.passport_num AS passport,
	concat_ws(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
	t1.phone AS phone,
	'diff_town' AS event_type,
	t5.terminal_city,
	CAST(current_timestamp AS timestamp)  AS report_dt
from clients t1
left JOIN accounts t2
ON t1.client_id = t2.client
left JOIN cards t3
ON t2.account = t3.account
left JOIN dwh_fact_transaction t4
ON t3.card_num = t4.card_num
LEFT JOIN dwh_dim_terminals t5
ON t4.terminal = t5.terminal_id;""")
        cursor.execute("""DROP VIEW IF EXISTS v_diff_town;""")
        # Целевое представление
        cursor.execute("""CREATE VIEW v_diff_town as
SELECT 
	t1.event_dt,
	t1.passport,
	t1.fio,
	t1.phone,
	t1.event_type,
	t1.report_dt
FROM 	(SELECT
		*,
		LAG(event_dt) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_event_dt,
		LAG(terminal_city) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_terminal_city
		FROM v_tmp_diff_town
		) AS t1
-- Фильтруем транзакции произошедшие в разных городах одним клиентом в течение 1 часа
WHERE t1.prev_terminal_city != t1.terminal_city
AND (t1.event_dt - INTERVAL '1 hour') < prev_event_dt;""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция v_diff_town успешно загрузила данные в представление v_diff_town')
    except Exception:
        connection.rollback()
        print('Функции v_diff_town не удалось загрузить данные в представление v_diff_town')


def v_amount_selection():
    """Функция создает представление v_amount_selection с операциями подбора
    суммы в течение 20 минут"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""DROP VIEW IF EXISTS v_tmp_amount_selection;""")
        # Временное представление
        cursor.execute("""CREATE VIEW v_tmp_amount_selection AS 
SELECT 
	t4.transaction_date AS event_dt,
	t1.passport_num AS passport,
	concat_ws(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
	t1.phone AS phone,
	'amount_selection' AS event_type,
	t4.amount,
	t4.oper_result,
	CAST(current_timestamp AS timestamp)  AS report_dt
from clients t1
left JOIN accounts t2
ON t1.client_id = t2.client
left JOIN cards t3
ON t2.account = t3.account
left JOIN dwh_fact_transaction t4
ON t3.card_num = t4.card_num;""")
        cursor.execute("""DROP VIEW IF EXISTS v_amount_selection;""")
        # Целевое представление
        cursor.execute("""CREATE VIEW v_amount_selection AS
SELECT 
	t1.event_dt,
	t1.passport,
	t1.fio,
	t1.phone,
	t1.event_type,
	t1.report_dt
FROM 	(SELECT
		*,
		LAG(amount, 1) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_amount_1,
		LAG(amount, 2) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_amount_2,
		LAG(amount, 3) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_amount_3,
		LAG(oper_result, 1) OVER(PARTITION BY passport ORDER BY event_dt) AS oper_result_1,
		LAG(oper_result, 2) OVER(PARTITION BY passport ORDER BY event_dt) AS oper_result_2,
		LAG(oper_result, 3) OVER(PARTITION BY passport ORDER BY event_dt) AS oper_result_3,
		LAG(event_dt, 3) OVER(PARTITION BY passport ORDER BY event_dt) AS prev_event_dt_3
		FROM v_tmp_amount_selection
		) AS t1
-- Фильтруем транзакции,соответствующие условиям Fraud подбора суммы
WHERE t1.prev_amount_1 > t1.amount 
AND t1.prev_amount_2 > t1.prev_amount_1 
AND t1.prev_amount_3 > t1.prev_amount_2
AND prev_event_dt_3 + INTERVAL '20 minute' > event_dt
AND oper_result = 'SUCCESS'
AND oper_result_1 = 'REJECT'
AND oper_result_2 = 'REJECT'
AND oper_result_3 = 'REJECT';""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция v_amount_selection успешно загрузила данные в представление v_amount_selection')
    except Exception:
        connection.rollback()
        print('Функции v_amount_selection не удалось загрузить данные в представление v_amount_selection')


def rep_fraud():
    """Функция создает таблицу(витрину) rep_fraud из 4-х представлений 
    с fraud операциями, а затем удаляет все временные представления"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Создаем таблицу
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""create table IF not EXISTS rep_fraud(
                       	event_dt timestamp,
                        passport varchar(32),
                        fio varchar(128),
                        phone varchar(32),
                        event_type varchar(32),
                        report_dt timestamp);""")
        # Наполняем таблицу rep_fraud данными из представлений
        cursor.execute("""INSERT INTO rep_fraud(
                       event_dt,
                       passport,
                       fio,
                       phone,
                       event_type,
                       report_dt)
                       SELECT * from v_invalid_or_blacklist_passport
                       union all
                       SELECT * from v_invalid_account
                       union all
                       SELECT * from v_diff_town
                       union all
                       SELECT * from v_amount_selection
                       """)
        cursor.execute("""DROP VIEW v_tmp_amount_selection cascade;""")
        cursor.execute("""DROP VIEW v_tmp_diff_town cascade;""")
        cursor.execute("""DROP VIEW v_invalid_account;""")
        cursor.execute("""DROP VIEW v_invalid_or_blacklist_passport;""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция rep_fraud успешно загрузила данные в таблицу rep_fraud')
        print('Функция rep_fraud успешно удалила временные представления')
    except Exception:
        connection.rollback()
        print('Функции rep_fraud не удалось загрузить данные в таблицу rep_fraud')


def meta_fraud():
    """Функция создает таблицу meta_fraud с метаданными fraud операций"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Создаем таблицу
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""create table IF not EXISTS meta_fraud(
                       event_date date,
                       type_fraud varchar(64),
                       count_fraud_operation integer,
                       report_dt timestamp);""")
        cursor.execute("""INSERT INTO meta_fraud(
                       event_date,
                       type_fraud,
                       count_fraud_operation,
                       report_dt)
                       select
                       date_trunc('day', event_dt) as event_date,
                       event_type as type_fraud,
                       count(*) as count_fraud_operation,
                       report_dt
                       from rep_fraud
                       group by date_trunc('day', event_dt), type_fraud, report_dt
                       order by event_date""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция meta_fraud успешно загрузила данные в таблицу meta_fraud')
    except Exception:
        connection.rollback()
        print('Функции meta_fraud не удалось загрузить данные в таблицу meta_fraud')
