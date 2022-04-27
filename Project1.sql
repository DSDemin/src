
/* Создаем VIEW */
DROP VIEW IF EXISTS analysis.orderitem;
CREATE VIEW analysis.orderitem AS 
SELECT * FROM production.orderitems o;

DROP VIEW IF EXISTS analysis.orders;
CREATE VIEW analysis.orders AS 
SELECT * FROM production.orders o;

DROP VIEW IF EXISTS analysis.orderstatuses;
CREATE VIEW analysis.orderstatuses AS 
SELECT * FROM production.orderstatuses o;

DROP VIEW IF EXISTS analysis.products;
CREATE VIEW analysis.products AS 
SELECT * FROM production.products o;

DROP VIEW IF EXISTS analysis.users;
CREATE VIEW analysis.users AS 
SELECT * FROM production.users o;

/* Создаем Будущую витрину */
DROP TABLE IF EXISTS analysis.dm_rfm_segments; 
CREATE TABLE analysis.dm_rfm_segments 
(
user_id int4 NOT NULL
,recency smallint NOT NULL
,frequency smallint NOT NULL
,monetary_value smallint NOT NULL
,CONSTRAINT users_pkey PRIMARY KEY (user_id)
);

/*
 *Для расчета витрины будет использована таблица production.orders  
 * Столбцы order_ts - мин дата 2022-02-12, максимальная 2022-03-14, пропусков нет
 * 
 * payment - миз значение 60 макс 6 360 - что вполне нормально, пропусков нет
 * Для обеспечения качества данных используется:
 * первичный ключ - order_id
 * дополнительная проверка, что сумма bonus_payment + payment = cost
 * Данные не могут быть пустыми
 * Дефолтное значение устанавливается как 0.
 * Отсюда возникает вопрос - атрибут bonus_payment содержит только дефолтные? значения 0 - возможно это ошибка 
 * на источнике, стоит уточнить. 
 */

/* Заливаем данные */
INSERT INTO analysis.dm_rfm_segments 
WITH users AS (
	SELECT 
			DISTINCT u.id AS user_id 
	FROM analysis.users u
	LEFT JOIN analysis.orders o
	ON u.id = o.user_id
	WHERE o.status IN (4,5))
,Recency AS (
	SELECT 
			o.user_id
			,max(o.order_ts) AS order_ts
	FROM users u
	LEFT JOIN analysis.orders o 
	ON u.user_id = o.user_id
	GROUP BY o.user_id 
	ORDER BY 2 DESC)
,Frequency AS (
	SELECT 
			o.user_id
			,count(o.order_ts) AS cnt
	FROM users u
	LEFT JOIN analysis.orders o
	ON u.user_id = o.user_id
	AND o.status = 4
	GROUP BY o.user_id 
	ORDER BY 2 DESC)
,Monetary AS (
	SELECT 
			u.user_id
			,sum(o.cost) AS total
		FROM users u
		LEFT JOIN  analysis.orders o
		ON u.user_id = o.user_id
		AND o.status = 4
		GROUP BY u.user_id 
		ORDER BY 2 DESC
) SELECT 
	u.user_id 
	,ntile(5) over(order by r.order_ts desc nulls first)  as "Recency"
	,ntile(5) over(order by f.cnt desc nulls first)  as "Frequency"
	,ntile(5) over(order by m.total desc nulls first)  as "Monetary"
FROM users u
LEFT JOIN Recency r 
ON u.user_id = r.user_id 
LEFT JOIN Frequency f 
ON u.user_id = f.user_id 
LEFT JOIN Monetary m 
ON u.user_id = m.user_id
ORDER BY user_id desc; 

/* Проверка распределений значений по группам */

--SELECT 
--distinct(recency)
--,count(*)
--FROM analysis.dm_rfm_segments drs 
--GROUP BY 1

--SELECT 
--distinct(Frequency)
--,count(*)
--FROM analysis.dm_rfm_segments drs 
--GROUP BY 1

--SELECT 
--distinct(Monetary_value)
--,count(*)
--FROM analysis.dm_rfm_segments drs 
--GROUP BY 1

-- все ок

DROP VIEW IF EXISTS analysis.orders;
CREATE VIEW analysis.orders AS 
SELECT 
	DISTINCT o.order_id 
	,o.order_ts
	,o.user_id
	,o.bonus_payment 
	,o.payment 
	,o."cost" 
	,o.bonus_grant 
	,ol.status_id AS status
FROM production.orders o 
LEFT JOIN (SELECT DISTINCT order_id, max(dttm) AS dttm, status_id FROM production.OrderStatusLog 
		GROUP BY order_id, status_id) ol 
ON o.order_id = ol.order_id
AND o.order_ts = ol.dttm;