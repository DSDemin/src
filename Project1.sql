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
--Добавил проверку на атрибуты RFM
DROP TABLE IF EXISTS analysis.dm_rfm_segments; 
CREATE TABLE analysis.dm_rfm_segments 
(
user_id int4 NOT NULL PRIMARY KEY 
,recency smallint NOT NULL CHECK (recency BETWEEN 1 AND 5)
,frequency smallint NOT NULL CHECK (frequency BETWEEN 1 AND 5)
,monetary_value smallint NOT NULL CHECK (monetary_value BETWEEN 1 AND 5)
);

/*
 *Для расчета витрины будет использована таблица production.orders  
 * Столбцы order_ts - мин дата 2022-02-12, максимальная 2022-03-14, пропусков нет но и данных за 2021г тоже нет
 * Столбец status - пропусков нет, можно объединять с таблицей статусов 
 * cost - мин значение 60 макс 6 360 - что вполне нормально, пропусков нет
 * order_ts - мин значение 2022-02-12, максимальное 2022-03-14, пропусков нет,
 * Для обеспечения качества данных используется:
 * первичный ключ - order_id
 * дополнительная проверка, что сумма bonus_payment + payment = cost
 * Данные не могут быть пустыми
 * Дефолтное значение устанавливается как 0.
 * Отсюда возникает вопрос - атрибут bonus_payment содержит только дефолтные? значения 0 - возможно это ошибка 
 * на источнике, стоит уточнить. 
 */


/* Заливаем данные */
/* Что тут происходит:
 * в первом СТЕ USERS отбираются все уникальные user_id согласно ТЗ
 * 1) дата заказа 2021 год и дальше
 * 2) Для расчета показателя Receny требуются статусы Closed и Cancelled - поэтому выбираются эти статусы.
 * Далее по коду, для расчета показателей Frequency и Monteray мы будем делать left join от этого CTE к подзапросу,
 * в котором будем выбирать только успешные заказы (статус Closed). Т.о мы будем сохранять консистентность данных между 3-мя 
 * показателями, а для последних 2-х заполним пропуски минимальными значениями.
 * Схема для users указываться не будет - т.к это CTE  
 * 
 * По твоим комментариям я переписал запрос:
 * 1) Добавил везде условие на отбор заказов по дате
 * 2) Обработал NULL с помощью COALESCE
 * 3) Объединил запросы Frequency и Monteray в один 
 * 4) Поменял группировку в финальной части
 * 5) Менять схему нигде не стал, я описал свою логику выше + я писал куратору что некорректно рассчитывать 
 * потраченную сумму клиентам, у которых статус заказа только Cancelled - пример user_id 977  
 *
 */
INSERT INTO analysis.dm_rfm_segments 
WITH users AS (
	SELECT 
			DISTINCT u.id AS user_id 
	FROM analysis.users u
	LEFT JOIN analysis.orders o
	ON u.id = o.user_id
	LEFT JOIN analysis.orderstatuses os 
	ON o.status = os.id AND os."key" IN ('Closed','Cancelled')
	WHERE extract(YEAR FROM o.order_ts) >= 2021
	) 
,Recency AS (
	SELECT 
			u.user_id
			,COALESCE(max(o.order_ts),date'1991-01-01') AS order_ts -- на случай NULL 
	FROM users u
	LEFT JOIN analysis.orders o 
	ON u.user_id = o.user_id
	GROUP BY u.user_id 
	ORDER BY 2 DESC)
,Frequency AS (
	SELECT 
			u.user_id
			,COALESCE (count(o.order_ts),0) AS cnt -- на случай NULL 
			,COALESCE (sum(o."cost"),0) AS total_amt -- на случай NULL 
	FROM users u
	LEFT JOIN ( SELECT 
					* 
				FROM analysis.orders o 
				INNER JOIN analysis.orderstatuses os 
				ON o.status = os.id AND os."key" = 'Closed') o
	ON u.user_id = o.user_id
	GROUP BY u.user_id 
	ORDER BY 1 DESC)
 SELECT 
	u.user_id 
	,ntile(5) over(order by r.order_ts asc)  as "Recency"
	,ntile(5) over(order by f.cnt asc)  as "Frequency"
	,ntile(5) over(order by f.total_amt asc)  as "Monetary"
FROM users u
LEFT JOIN Recency r 
ON u.user_id = r.user_id 
LEFT JOIN Frequency f 
ON u.user_id = f.user_id 
ORDER BY user_id desc; 

/*
 * вариант №2 что бы собрать данные
 * WITH RFM AS (
SELECT 
	user_id
	,max(CASE WHEN os."key" IN ('Closed','Cancelled') THEN order_ts END)  AS Recency
	,sum(CASE WHEN os."key" IN ('Closed') THEN 1 ELSE 0 END) AS Frequency
	,sum(CASE WHEN os."key" IN ('Closed') THEN "cost" else 0 end) AS Monetary
FROM production.orders o 
LEFT JOIN production.orderstatuses os 
ON o.status = os.id
GROUP BY user_id)
 SELECT 
	a.user_id 
	,ntile(5) over(order by a.Recency asc)  as "Recency"
	,ntile(5) over(order by a.Frequency asc)  as "Frequency"
	,ntile(5) over(order by a.Monetary asc)  as "Monetary"
FROM RFM 
 */

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
--GROUP BY 1;

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