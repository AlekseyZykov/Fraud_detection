import psycopg2
import json


# Читаем файл cred, в котором хранятся данные для подключения
with open('cred.json', 'r', encoding='utf-8') as f:
    cred = json.load(f)


def transactions_to_dwh():
    """Функция преобразует и очищает данные в таблице stg_transactions
    и помещает их в целевую таблицу хранилища dwh_fact_transaction"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""CREATE TABLE if not exists dwh_fact_transaction(
                    transaction_id varchar(32),
                    transaction_date timestamp,
                    amount numeric(10, 2),
                    card_num varchar(32),
                    oper_type varchar(32),
                    oper_result varchar(32),
                    terminal varchar(32)
                    );
                    """)
        cursor.execute("""INSERT INTO dwh_fact_transaction(
                    transaction_id,
                    transaction_date,
                    amount,
                    card_num,
                    oper_type,
                    oper_result,
                    terminal
                    )
                    SELECT
                    cast (transaction_id as varchar(32)) AS transaction_id,
                    cast(transaction_date AS timestamp) AS transaction_date,
                    CAST(replace(amount, ',', '.') AS numeric(12, 2)) AS amout,
                    card_num,
                    oper_type,
                    oper_result,
                    terminal
                    FROM stg_transactions;
                    """)
        # Очищаем таблицу stg_transactions
        cursor.execute("""TRUNCATE TABLE stg_transactions""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция transactions_to_dwh успешно загрузила данные в таблицу dwh_fact_transaction')
    except Exception:
        connection.rollback()
        print('Функции transactions_to_dwh не удалось загрузить данные в таблицу dwh_fact_transaction')


def passport_blacklist_to_dwh():
    """Функция преобразует и очищает данные в таблице stg_passport_blacklist
    и помещает их в целевую таблицу хранилища dwh_fact_passport_blacklist"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        cursor.execute("""CREATE TABLE if not exists dwh_fact_passport_blacklist(
                    passport_num varchar(32),
                    entry_dt date
                    )
                    """)
        cursor.execute("""INSERT INTO dwh_fact_passport_blacklist (passport_num, entry_dt)
                    SELECT
                    passport,
                    date
                    from (select
                          passport,
                          date,         
                          row_number() over(Partition by passport order by date) rnk
                          from stg_passport_blacklist
                          ) as tmp
                    where  rnk = 1
                    """)
        # Очищаем таблицу stg_passport_blacklist
        cursor.execute("""TRUNCATE TABLE stg_passport_blacklist""")
        connection.commit()
        cursor.close()
        connection.close()
        print('Функция passport_blacklist_to_dwh успешно загрузила данные в таблицу dwh_fact_passport_blacklist')
    except Exception as e:
        connection.rollback()
        print('Функции passport_blacklist_to_dwh не удалось загрузить данные в таблицу dwh_fact_passport_blacklist')
        print(e)


def terminals_to_dwh():
    """Функция преобразует и очищает данные в таблице stg_terminals
    и помещает их в целевую таблицу хранилища dwh_dim_terminals"""

    try:
        # Устанавливаем подключение и создаем курсор
        connection = psycopg2.connect(**cred)
        cursor = connection.cursor()
        # Execute
        cursor.execute("""SET search_path TO bank;""")
        # Создаем временную таблицу tmp_dim_terminals из stg_terminals
        cursor.execute("""CREATE TABLE if not exists tmp_dim_terminals(
                       terminal_id varchar(32),
                       terminal_type varchar(16),
                       terminal_city varchar(32),
                       terminal_address varchar(128),
                       effective_from date,
                       effective_to date default (cast('5999-12-31' as date)),
                       deleted_flg integer default 0
                       );
                       """)
        # Наполняем таблицу tmp_dim_terminals
        cursor.execute("""INSERT INTO tmp_dim_terminals(
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from
                       )
                       SELECT
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from
                       FROM stg_terminals;
                       """)
        # Представление для преобразования данных
        cursor.execute("""drop view if exists v_transform_stg_terminals;""")
        cursor.execute("""create view v_transform_stg_terminals as
                       SELECT
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from,
                       CASE 
                       when terminal_address = LEAD(terminal_address) over(Partition by terminal_id order by effective_from)
                       or LEAD(terminal_address) over(Partition by terminal_id order by effective_from) is NULL then cast('5999-12-31' as date)
                       else effective_from
                       end as effective_to,
                       case 
                       when terminal_address = LEAD(terminal_address) over(Partition by terminal_id order by effective_from)
                       or LEAD(terminal_address) over(Partition by terminal_id order by effective_from) is NULL then 0
                       else 1
                       end as deleted_flg
                       FROM tmp_dim_terminals;
                       """)
        # Представление для очистки от дубликатов
        cursor.execute(
            """drop view if exists v_deduplication_stg_terminals;""")
        cursor.execute("""create view v_deduplication_stg_terminals as
                       SELECT
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from,
                       effective_to,
                       deleted_flg,
                       deduplication_row
                       FROM (SELECT 
                            *,
                            row_number() over(Partition by terminal_id, terminal_address, deleted_flg order by effective_from) as deduplication_row
                            FROM v_transform_stg_terminals)
                       """)
        # Создаем целевую таблицу dwh_dim_terminals
        cursor.execute("""CREATE TABLE if not exists dwh_dim_terminals(
                       terminal_id varchar(32),
                       terminal_type varchar(16),
                       terminal_city varchar(32),
                       terminal_address varchar(128),
                       effective_from date,
                       effective_to date,
                       deleted_flg integer
                       );
                       """)
        # Наполняем таблицу dwh_dim_terminals из v_deduplication_stg_terminals
        cursor.execute("""INSERT INTO dwh_dim_terminals(
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from,
                       effective_to,
                       deleted_flg
                       )
                       SELECT
                       terminal_id,
                       terminal_type,
                       terminal_city,
                       terminal_address,
                       effective_from,
                       effective_to,
                       deleted_flg
                       FROM v_deduplication_stg_terminals
                       WHERE deduplication_row = 1 or deleted_flg = 1;
                       """)
        # Удаляем представления и временные таблицы
        cursor.execute("""DROP TABLE if exists tmp_dim_terminals CASCADE""")
        # Очищаем таблицу stg_terminals
        cursor.execute("""TRUNCATE TABLE stg_terminals""")
        connection.commit()
        cursor.close()
        connection.close()
        print(
            'Функция terminals_to_dwh успешно загрузила данные в таблицу dwh_dim_terminals')
    except Exception as e:
        connection.rollback()
        print('Функции terminals_to_dwh не удалось загрузить данные в таблицу dwh_dim_terminals')
        print(e)
