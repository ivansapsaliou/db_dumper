# DB Dump Manager

Менеджер резервных копий баз данных — PostgreSQL, MySQL, Oracle.  
Работает на **Windows** без установки клиентских утилит (pg_dump, mysqldump и т.д.).

---

## Требования

- **Python 3.10+** — https://www.python.org/downloads/
- Интернет для первичной установки зависимостей

---

## Быстрый старт (Windows)

```
1. Распакуйте папку dbdump куда угодно
2. Дважды щёлкните start.bat
   → При первом запуске автоматически создаётся .venv и ставятся зависимости
   → Браузер откроется на http://localhost:5000
```

---

## Ручная установка

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -r requirements.txt
python app.py
```

---

## Функции

| Функция | Описание |
|---------|----------|
| **Управление БД** | Добавление/редактирование/удаление конфигураций |
| **Дамп без клиента** | Прямое подключение через Python-драйверы |
| **SSH-туннель** | Дамп через SSH без клиентских утилит на сервере |
| **Прогресс в реальном времени** | WebSocket (Socket.IO) |
| **Скачивание** | Прямо из браузера |
| **Расписание (cron)** | APScheduler, cron-выражения |
| **Проверка диска** | Перед каждым дампом |
| **История** | Все выполненные дампы |
| **Настройки** | Путь сохранения, лимит истории |

---

## Поддерживаемые БД

### PostgreSQL
- Прямое: psycopg2
- SSH: команда `pg_dump` на сервере (если есть), иначе Python

### MySQL / MariaDB
- Прямое: PyMySQL
- SSH: команда `mysqldump` на сервере

### Oracle
- Прямое: cx_Oracle (нужен Oracle Instant Client на локальной машине)
- SSH: Python + cx_Oracle на сервере

---

## Режимы дампа

- **Полный** — схема + данные (по умолчанию)
- **Только схема** — CREATE TABLE без INSERT
- **Только данные** — INSERT без CREATE TABLE
- **Выборочные таблицы** — include / exclude списки

---

## SSH-подключение

Заполните поля SSH в форме добавления БД:

```
SSH Host: 10.0.0.5
SSH Port: 22
SSH User: ubuntu
SSH Password: *** (или укажите путь к ключу)
SSH Key:  ~/.ssh/id_rsa
```

При использовании SSH — дамп выполняется командой на сервере,  
результат передаётся по SFTP на локальную машину.

---

## Cron-выражения (примеры)

```
0 2 * * *      — каждый день в 02:00
0 */6 * * *    — каждые 6 часов
0 3 * * 0      — каждое воскресенье в 03:00
30 1 * * 1-5   — в 01:30 по будням
0 0 1 * *      — 1-го числа каждого месяца
```

---

## Структура файлов

```
dbdump/
├── app.py            — Flask-приложение, маршруты API
├── db_dumper.py      — Движок дампа (PG / MySQL / Oracle)
├── config_manager.py — Хранилище конфигурации (JSON)
├── requirements.txt  — Зависимости Python
├── config.json       — Создаётся автоматически
├── start.bat         — Запуск на Windows
├── templates/
│   └── index.html    — Веб-интерфейс (single-page app)
└── dumps/            — Папка дампов по умолчанию
```

---

## Примечание по Oracle

Oracle требует **Oracle Instant Client** для cx_Oracle.  
Скачать: https://www.oracle.com/database/technologies/instant-client.html  
После распаковки добавьте путь в `PATH`.

При использовании **SSH-режима** cx_Oracle нужен только на **сервере**.
