-- Задание №1

SELECT
	campaign_name,
	COUNT(event_type) AS num_opens,
	COUNT(DISTINCT contact_key) AS num_clients
FROM
	campaign_delivery
WHERE
	event_type = 'Open'
GROUP BY
	campaign_name;


-- Задание №2

SELECT
	campaign_name,
	num_opens,
	num_clients
FROM
	(
	SELECT
		campaign_name,
		CAST(event_date AS timestamp) AS event_datetime,
		COUNT(event_type) AS num_opens,
		COUNT(DISTINCT contact_key) AS num_clients,
		MIN(CAST(event_date AS timestamp)) OVER(PARTITION BY contact_key ORDER BY campaign_name) AS min_event_date
	FROM
		campaign_delivery
	WHERE
		event_type = 'Open'
	GROUP BY
		campaign_name) AS subquery
WHERE
	event_datetime = min_event_date;


-- Задание №3

SELECT
	campaign_name,
	num_opens,
	num_clients_open,
	num_deliveries,
	num_clients_delivery
FROM
		(
		SELECT
			campaign_name,
			CAST(event_date AS timestamp) AS event_datetime_op,
			COUNT(event_type) AS num_opens,
			COUNT(DISTINCT contact_key) AS num_clients_open,
			MIN(CAST(event_date AS timestamp)) OVER(PARTITION BY contact_key ORDER BY campaign_name) AS min_event_date_op
		FROM
			campaign_delivery
		WHERE
			event_type = 'Open'
		GROUP BY
			campaign_name) AS sq_opens 
	RIGHT JOIN -- RIGHT, так как все открытые сообщения были доставлены, но не факт, что все доставленые были открыты
		(
		SELECT
			campaign_name,
			CAST(event_date AS timestamp) AS event_datetime_del,
			COUNT(event_type) AS num_deliveries,
			COUNT(DISTINCT contact_key) AS num_clients_delivery,
			MIN(CAST(event_date AS timestamp)) OVER(PARTITION BY contact_key ORDER BY campaign_name) AS min_event_date_del
		FROM
			campaign_delivery
		WHERE
			event_type = 'Sent'
		GROUP BY
			campaign_name) AS sq_deliveries 
	ON sq_opens.campaign_name = sq_deliveries.campaign_name
WHERE
	event_datetime_op = min_event_date_op
	AND event_datetime_del = min_event_date_del;


-- Задание №4

SELECT
	name_letter,
	num_opens,
	num_letters
FROM
	(
	SELECT
		SUBSTRING_INDEX(communication_element.comm_elem_name, '_', 1) AS name_letter, -- или попробовать так - SUBSTR(нужое поле, 1, STRPOS(нужное поле, '_')-1)
		COUNT(campaign_delivery.event_type) AS num_opens,
		COUNT(DISTINCT campaign_delivery.src_campaign_delivery_id) AS num_letters,  /*  считаем уникальные значения этого поля, так как они, судя по всему, 
																						являются уникальными для каждого письма. Предположу, что так и было задумано */
		CAST(campaign_delivery.event_date AS timestamp) AS event_datetime,
		MIN(CAST(campaign_delivery.event_date AS timestamp)) OVER(PARTITION BY campaign_delivery.contact_key ORDER BY campaign_delivery.campaign_name) AS min_event_date
	FROM
		campaign_delivery
		LEFT JOIN contact_communication_element ON contact_communication_element.src_contact_comm_elem_id = campaign_delivery.src_campaign_delivery_id  /*  в таблице "campaign_delivery" поле, содержащее внешний ключ, 
																																							оказалось повреждено, джойнить по "contact_key" будет проблемно, 
																																							потому что будет задвоение строк */
		LEFT JOIN communication_element ON communication_element.comm_elem_key = contact_communication_element.comm_elem_key
	WHERE
		campaign_delivery.event_type = 'Open'
	GROUP BY
		SUBSTRING_INDEX(communication_element.comm_elem_name, '_', 1)) AS subquery
WHERE
	event_datetime = min_event_date;


-- Задание №5

SELECT
	contact_key, 
	first_name, 
	last_name, 
	email, 
	vehicle_key, 
	vehicle_name, 
	relationship_start_date
FROM
	(
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY contact_key ORDER BY relationship_start_date DESC) AS row_num
		FROM
			invoices
			LEFT JOIN contacts ON contacts.contact_key = invoices.contact_key
		WHERE
			brand = 'BMW') AS subquery
WHERE
	row_num <= 3
ORDER BY
	contact_key, 
	relationship_start_date;
