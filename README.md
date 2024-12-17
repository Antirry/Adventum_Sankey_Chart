# Добро пожаловать в проект Sankey Chart для Adventum стажировки

Проект создается по таблице **events** в **clickhouse** базе данных.

Выполнялся в качестве задания https://analyticspace.adventum.ru/datacraft И Adventum adventum.ru

## Что включает себя проект?

### Файл .ini

    Файл, который включает себя подключение к базе данных Clickhouse в форме:
    
    [Default-Clickhouse]
    username=
    host=
    password=
    port=
    -- Имя базы данных:
    database=

    И пути до файлов для функции создания запроса SQL:

    [Default-Query]
    name_yml= Название yml файла, для переменных запроса
    name_folder= Название папки для файла SQL
    name_query= Название файла в папке SQL формата .sql

### Файл .yml

    Файл, который включает словарь переменных:

    ОБЯЗАТЕЛЬНО ИМЕЕТ

        vars:
            see_dates:
            table_name:
            time_after:
            time_before:
            include_events:
            session_time:
            max_step_target:
            end_of_path:

    Подробнее в самом файле.

# Важно сохранять иерархию проекта

Важно сохранять структуру проекта в таком же виде, что и ниже

[Для отображения схемы в VS CODE](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid)

[Как выглядит проект (картинка)](project/parts/png_md/Как%20выглядит%20проект.png)

```mermaid
graph LR;
A[Папка проекта]-->B; 
B[project]-->C[parts 'Файлы проекта'];
B-->D[Конфиг файлы '.ini, .yml', Файл запуска];
A-->E[Результат .html]
```

# Как выглядит запрос:

```mermaid
graph TB;

A[sankey_source 

appmetrica_device_id, session_id, event_name, event_datetime

'Источник']-->B[enumerate_steps 

qid, event_datetime, numbered_event_name 

'Нумерование шагов'];
B-->C[sessions_division 

qid, event_datetime, numbered_event_name, period_check, session, new_session

'Разделение по сессиям'];
C--Одинаковые столбцы-->D[delete_events *

'Оставление данных только в диапазоне дат'];
C--Одинаковые столбцы-->E[generate_ALL_events *

'Генерирование полного пути,
на чем остановился пользователь'];
C--Одинаковые столбцы-->F[generate_event *

'Создание ивента выходящий за диапазон дат'];
C--Одинаковые столбцы-->G[keep_events * 

'Сохранение только тех сессий,
что входят в диапазон дат хотя бы частью'];
C--Одинаковые столбцы-->H[keep_only_entire * 

'Сохранение тех сессий, что начались в период дат'];
E & D & F & G & H-->I[include_events * Выбирается через if режим 

'Тип фильтрации'];
I--Одинаковые столбцы-->J[add_end_of_path *

'Добавление к концу пути end_of_path'
];
J-->K[prev
qid, event_datetime, period_check, source, target

'Определение source & target'];
K-->L[cnt_dest_source
source, target, cnt

'Итоговая таблица'];
```

# Последовательность действий работы проекта:

```mermaid
graph TB;

A[Запуск файла Chart_sankey]-->
B[Запуск файла _extract_config]-->
C[Извлечение данных из конфиг .ini]-->
D[Соединение с базой CLickhouse]-->
E[Запуск файла index]-->
F[Создание запроса STRING .sql]-->
G[Создание dataframe из запроса]-->
H[Создание графика Sankey]-->
I[Сохранение в .html]
```

# Откуда поступают данные:

```mermaid
graph LR

A{Запуск программы
Chart_sankey.py} --> B{Чтение конфиг файла .ini
Пример: MyConfig.ini} --> C[Папка с .sql файлом
Пример: Sankey Query] --> D[Файл .sql
Пример: code_sankey_query.sql];

```