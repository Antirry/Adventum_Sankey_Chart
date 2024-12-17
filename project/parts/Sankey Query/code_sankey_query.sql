{% set include_events = var("include_events") %}
{% set sort_numbered_event = "LEFT(numbered_event_name, POSITION(numbered_event_name, '_') - 1)" %}
{% set sort_target = "LEFT(target, POSITION(target, '_') - 1)" %}

WITH

    -- Парсинг даты и формирование источника
    sankey_source AS (
        SELECT
            appmetrica_device_id,
            session_id,
            event_name,
            parseDateTimeBestEffort(event_datetime) AS event_datetime
        FROM
            {{var('table_name')}}
    ),

    -- Нумерование шагов
    enumerate_steps AS (
        SELECT
            CONCAT(appmetrica_device_id, '_', session_id) AS qid,
            event_datetime,
            CONCAT(ROW_NUMBER() OVER (PARTITION BY qid ORDER BY event_datetime ASC), '_', event_name) AS numbered_event_name
        FROM
            sankey_source
    ),

    -- Разделение по сессиям
    sessions_division AS (
        SELECT 
            qid,
            event_datetime,
            numbered_event_name,
            period_check,
            (SUM(new_session) OVER (PARTITION BY qid ORDER BY {{sort_numbered_event}}::INT ASC)) + 1 AS session,
            new_session
        FROM
        (
            SELECT *,
                if(session_time > {{var('session_time')}}::INT, 1, 0) AS new_session
            FROM
            (
                SELECT *,
                    if(toDate(event_datetime) < toDate('{{var("time_after")}}') AND
                        toDate(event_datetime) > toDate('{{var("time_before")}}') OR 
                        (toDate(event_datetime) = toDate('{{var("time_after")}}') AND
                        toDate(event_datetime) = toDate('{{var("time_before")}}')), true, false
                    ) AS period_check,
                    greatest(COALESCE(date_diff('hour', lagInFrame(event_datetime)
                     OVER (PARTITION BY qid ORDER BY 
                     {{sort_numbered_event}}::INT ASC), 
                     event_datetime), 0), 0) AS session_time
                FROM
                    enumerate_steps
                ORDER BY {{sort_numbered_event}}::INT ASC
            )
        )
    ORDER BY event_datetime DESC
    ),

    -- Оставление данных только в диапазоне дат
    delete_events AS (
        SELECT * FROM sessions_division
        WHERE period_check
        -- AND qid = '951455167552675482_10000000002'
        ORDER BY event_datetime ASC
    ),

    -- Генерирование полного пути,
        -- на чем остановился пользователь
    generate_ALL_events AS (
        WITH example_table AS (
            SELECT 1 AS id, 'start' AS name UNION ALL
            SELECT 2, 'screen_view' UNION ALL
            SELECT 3, 'select_content' UNION ALL
            SELECT 4, 'view_cart' UNION ALL
            SELECT 5, 'add_to_cart' UNION ALL
            SELECT 6, 'begin_checkout' UNION ALL
            SELECT 7, 'purchase'
        ),
        last_event AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY qid, session ORDER BY {{sort_numbered_event}}::INT DESC) AS rn
            FROM delete_events
        ),
        last_event_extract AS (
            SELECT 
                qid,
                event_datetime,
                numbered_event_name AS last_event_number,
                period_check,
                session,
                new_session
            FROM last_event
            WHERE rn = 1
        ),
        join_event AS (
            SELECT *,
            FROM last_event_extract
            CROSS JOIN 
            example_table
        )
        SELECT * FROM
        (
            SELECT
                qid,
                event_datetime,
                CONCAT((ROW_NUMBER() OVER (PARTITION BY qid, session ORDER BY id ASC)) +
                LEFT(last_event_number, POSITION(last_event_number, '_') - 1)::INT, '_', name) AS numbered_event_name,
                period_check,
                session,
                new_session
            FROM
            (
                SELECT *,
                    FIRST_VALUE(numbered_event) OVER (PARTITION BY qid, session) AS fv_event
                FROM
                (
                    SELECT *,
                        (
                            CASE WHEN RIGHT(
                                last_event_number,
                                LENGTH(last_event_number) - LENGTH(LEFT(last_event_number, POSITION(last_event_number, '_')))
                            ) = name THEN id END
                        ) AS numbered_event
                    FROM join_event
                ) AS number_event
            )
            WHERE fv_event < id
        ) AS res
        -- WHERE qid = '9514551675526775482_10000000002'
        UNION ALL
        SELECT * FROM delete_events
        -- WHERE qid = '9514551675526775482_10000000002'
    ),

    -- Создание ивента выходящий за диапазон дат
    generate_events AS (
        SELECT
            qid,
            event_datetime,
            CONCAT({{sort_numbered_event}}, '_out_of_range')
            AS numbered_event_name,
            period_check,
            session,
            new_session
        FROM
        (
            SELECT *,
                if(numbered_event_name = FIRST_VALUE(numbered_event_name)
                    OVER (PARTITION BY qid, session ORDER BY
                        {{sort_numbered_event}}::INT ASC),
                    null,
                    lagInFrame(period_check) 
                        OVER (PARTITION BY qid, session ORDER BY 
                            {{sort_numbered_event}}::INT ASC)
                )
                AS first_event,
                if(numbered_event_name = LAST_VALUE(numbered_event_name)
                    OVER (PARTITION BY qid, session ORDER BY
                        {{sort_numbered_event}}::INT ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 
                    null,
                    leadInFrame(period_check) 
                        OVER (PARTITION BY qid, session ORDER BY 
                        {{sort_numbered_event}}::INT ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                )
                AS last_event
            FROM
                sessions_division
        )
        WHERE
            last_event IS NOT NULL
            AND first_event IS NOT NULL
            AND ((NOT period_check AND first_event) OR (NOT period_check AND last_event))
        UNION ALL
        SELECT * FROM delete_events
    ),

    -- Сохранение только тех сессий, 
        -- что входят в диапазон хотя бы частью
    keep_events AS (
        SELECT * FROM
            sessions_division
        WHERE (session, qid) IN (
            SELECT DISTINCT session, qid FROM
            sessions_division
            WHERE period_check
            ORDER BY event_datetime ASC
        )
    ),

    -- Сохранение тех сессий, что начались в период дат
    keep_only_entire AS (
        SELECT * FROM
            sessions_division
        WHERE period_check AND tuple(session, qid) NOT IN (
            SELECT DISTINCT session, qid FROM sessions_division
            WHERE toDate(event_datetime) < toDate('{{var("time_before")}}')
        )
        ORDER BY event_datetime ASC
    ),

    -- Выбор типа фильтрации
    include_events AS (
        {% if include_events == 'keep' %}
        SELECT * FROM keep_events

        {% elif include_events == 'delete' %}
        SELECT * FROM delete_events

        {% elif include_events == 'generate' %}
        SELECT * FROM generate_events

        {% elif include_events == 'keep_entire' %}
        SELECT * FROM keep_only_entire

        {% elif include_events == 'generate_ALL' %}
        SELECT * FROM generate_ALL_events

        {% else %}
        SELECT 'Неверное значение'

        {% endif %}
    ),

    -- Добавление end_of_path к концу пути 
    add_end_of_path AS (

        {% if var('end_of_path') == True %}

        WITH example_table AS (
            SELECT 'end_of_path' AS name
        )
        SELECT * FROM 
        (
            SELECT 
                qid,
                event_datetime,
                if(last_event = numbered_event_name AND last_event NOT LIKE '%_out_of_range',
                    CONCAT(LEFT(last_event, POSITION(last_event, '_') - 1)::INT + 1,
                    '_', name), NULL)
                AS numbered_event_name,
                period_check,
                session,
                new_session
            FROM
            (
                SELECT 
                    qid,
                    event_datetime,
                    numbered_event_name,
                    period_check,
                    session,
                    new_session,
                    if(rn_desc = 1, numbered_event_name, '0_') AS last_event
                FROM
                (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY qid, session
                        ORDER BY {{sort_numbered_event}}::INT DESC)
                        AS rn_desc
                    FROM 
                        include_events
                ) AS rn
            ) AS l_event
            CROSS JOIN
            example_table
            WHERE numbered_event_name IS NOT NULL
        )
        -- WHERE qid = '951455167552675482_10000000002'
        UNION ALL
        SELECT * FROM include_events
        -- WHERE qid = '951455167552675482_10000000002'

        {% else %}

        SELECT * FROM include_events

        {% endif %}
    ),

    -- Новое нумерование и объявление как source, target
    prev AS (
        SELECT 
            qid,
            event_datetime,
            period_check,
            COALESCE(
                lagInFrame(target) OVER (PARTITION BY qid, session ORDER BY {{sort_target}}::INT ASC),
                if('1_out_of_range' = target, '0_out_of_range', '0_start'))
            AS source,
            target
        FROM 
        (
            SELECT *,
                CONCAT(ROW_NUMBER() OVER (PARTITION BY session, qid ORDER BY {{sort_numbered_event}}::INT ASC),
                '_', RIGHT(numbered_event_name, LENGTH(numbered_event_name) - LENGTH(LEFT(numbered_event_name, POSITION(numbered_event_name, '_'))))) AS target
            FROM
                add_end_of_path
            -- WHERE qid = '951455167552675482_10000000002'
        ) AS target_table
    ),


    -- Нахождение количества связей
    cnt_dest_source AS (
        SELECT * FROM
        (
            SELECT 
                source,
                target,
                COUNT(*) AS cnt
            FROM prev
            GROUP BY source, target
            HAVING 
                {{sort_target}}::INT < {{var('max_step_target')}} + 1
            ORDER BY 
                LEFT(source, POSITION(source, '_') - 1)::INT ASC
        )
        
        {% if include_events == 'generate' %}
        WHERE source NOT IN '0_out_of_range'
        UNION ALL
        SELECT 
            if(lagInFrame(target) OVER (ORDER BY {{sort_target}}::INT ASC) = '',
            '0_out_of_range',
            lagInFrame(target) OVER (ORDER BY {{sort_target}}::INT ASC)
            ) AS source,
            target,
            1 AS cnt
        FROM
        (
            {% for i in range(1, var('max_step_target')) %}
                SELECT CONCAT({{i}}, '_out_of_range') AS target
                UNION ALL
            {% endfor %}
            SELECT CONCAT({{var('max_step_target')}}, '_out_of_range')
        ) AS out_of_steps

        {% endif %}

        {% if var('end_of_path') == True %}

        UNION ALL
        SELECT 
            if(lagInFrame(target) OVER (ORDER BY {{sort_target}}::INT ASC) = '',
            '0_end_of_path',
            lagInFrame(target) OVER (ORDER BY {{sort_target}}::INT ASC))
            AS source,
            target,
            1 AS cnt
        FROM
        (
            {% for i in range(1, var('max_step_target'))%}
                SELECT CONCAT({{i}}, '_end_of_path') AS target
                UNION ALL
            {% endfor %}
            SELECT CONCAT({{var('max_step_target')}}, '_end_of_path') AS target
        ) end_of_path1

        {% endif %}
    )
{% if var('see_dates') == True %}

SELECT DISTINCT toDate(event_datetime) AS dates_dataset
FROM sankey_source

{% else %}

SELECT * FROM cnt_dest_source
-- WHERE 
--     source LIKE '%_out_of_range'
--     AND target LIKE '%_out_of_range'

{% endif %}