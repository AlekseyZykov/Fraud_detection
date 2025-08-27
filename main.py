import py_scripts.download_to_sql
import py_scripts.download_to_dwh
import py_scripts.rep_fraud


# Эта функция обрабатывает файлы passport_blacklist*.xlsx,
# загружает данные в postgresql в таблицу stg_passport_blacklist
# и перемещает обработанный файл в папку archive
py_scripts.download_to_sql.passport_blacklist_to_sql()

# Эта функция обрабатывает файлы terminals*.xlsx,
# загружает данные в postgresql в таблицу stg_terminals
# и перемещает обработанный файл в папку archive
py_scripts.download_to_sql.terminals_to_sql()

# Эта функция обрабатывает файлы transactions*.txt,
# загружает данные в postgresql в таблицу stg_transactions
# и перемещает обработанный файл в папку archive
py_scripts.download_to_sql.transactions_to_sql()

# Функция преобразует и очищает данные в таблице stg_transactions
# и помещает их в целевую таблицу хранилища dwh_fact_transaction
py_scripts.download_to_dwh.transactions_to_dwh()

# Функция преобразует и очищает данные в таблице stg_passport_blacklist
# и помещает их в целевую таблицу хранилища dwh_fact_passport_blacklist
py_scripts.download_to_dwh.passport_blacklist_to_dwh()

# Функция преобразует и очищает данные в таблице stg_terminals
# и помещает их в целевую таблицу хранилища dwh_dim_terminals
py_scripts.download_to_dwh.terminals_to_dwh()

# Функция создает представление v_invalid_or_blacklist_passport с операциями совершенными
# при просроченном или заблокированном паспорте
py_scripts.rep_fraud.v_invalid_or_blacklist_passport()

# Функция создает представление v_invalid_account с операциями совершенными
# при недействующем договоре
py_scripts.rep_fraud.v_invalid_account()

# Функция создает представление v_diff_town с операциями совершенными
# одним клиентом в разных городах в течение одного часа
py_scripts.rep_fraud.v_diff_town()

# Функция создает представление v_amount_selection с операциями подбора
# суммы в течение 20 минут
py_scripts.rep_fraud.v_amount_selection()

# Функция создает таблицу(витрину) rep_fraud из 4-х представлений
# с fraud операциями, а затем удаляет все временные представления
py_scripts.rep_fraud.rep_fraud()

# Функция создает таблицу meta_fraud с метаданными fraud операций
py_scripts.rep_fraud.meta_fraud()
