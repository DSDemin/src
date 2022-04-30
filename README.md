1.2. Изучите структуру исходных данных
Подключитесь к базе данных и изучите структуру таблиц.
- Основными таблицами при работе над проектом выступают:
- products.users - ключ к таблице по полю ID
- products.orders ключ к таблице по полю order_id, связь с таблицей users по условю: products.users.id = products.orders.user_id
- products.orederstatuses ключ к таблице по полю id, связь с таблицей orders по условию products.orders.id = products.orders.status



Создайте текстовой документ, в котором будете описывать решение. В этом же документе зафиксируйте, какие поля вы будете использовать для расчёта витрины.
--см SQL файд 

1.3. Проанализируйте качество данных

## Изучите качество входных данных.
см sql файл

 ## В итоговом документе опишите, насколько качественные данные хранятся в источнике.
 Подготовленные данные достаточно выского уровня качества.
 Вызывают вопросы следующие моменты:
 1) Где данные о заказах за 2021 год
 2) Заполненеие атрибута bonus_payment
## Укажите, какие инструменты для обеспечения качества данных использованы в таблицах в схеме production.
Для обеспечения качества данных используются проверки, ограничения и дефолтные значения в DDL таблиц

Для таблицы products.orders
Установка дефолтных значений:
bonus_payment numeric(19, 5) NOT NULL DEFAULT 0,
	payment numeric(19, 5) NOT NULL DEFAULT 0,
	"cost" numeric(19, 5) NOT NULL DEFAULT 0,
	bonus_grant numeric(19, 5) NOT NULL DEFAULT 0,
  
  Проверка:CONSTRAINT orders_check CHECK ((cost = (payment + bonus_payment))),
  
  Для всех атрибутов - условие NOT NULL
  
  Таблица users: для всех атрибутов условие NOT NULL
