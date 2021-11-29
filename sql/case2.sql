|********************************************************************************|
      SQL WITH MYSQL
|********************************************************************************|
WITH ventes_meuble AS (
    SELECT t.client_id AS client_id, SUM(t.prod_price*t.prod_qty) AS ventes_meuble
    FROM TRANSACTION t INNER JOIN PRODUCT_NOMENCLATURE p
    ON t.prod_id = p.product_id
	WHERE t.date BETWEEN CAST("2020-01-01" AS DATE) AND CAST("2020-12-31" AS DATE)
	AND p.product_type = "MEUBLE"
    GROUP BY t.client_id
),
ventes_deco AS (
    SELECT t.client_id AS client_id, SUM(t.prod_price*t.prod_qty) AS ventes_deco
    FROM TRANSACTION t INNER JOIN PRODUCT_NOMENCLATURE p
    ON t.prod_id = p.product_id
	WHERE t.date BETWEEN CAST("2020-01-01" AS DATE) AND CAST("2020-12-31" AS DATE)
	AND p.product_type = "DECO"
    GROUP BY t.client_id
),
ventes AS (
    SELECT m.client_id as meuble_client_id, m.ventes_meuble, d.client_id as deco_client_id, d.ventes_deco
    FROM ventes_meuble as m LEFT JOIN ventes_deco as d ON m.client_id = d.client_id
    UNION
    SELECT m.client_id as meuble_client_id, m.ventes_meuble, d.client_id as deco_client_id, d.ventes_deco
    FROM ventes_meuble as m RIGHT JOIN ventes_deco as d ON m.client_id = d.client_id
)
SELECT IFNULL(meuble_client_id, deco_client_id) AS client_id, ventes_meuble, ventes_deco
FROM ventes;

|********************************************************************************|
      RESULT WITH SQL LAUNCH
|********************************************************************************|

client_id	ventes_meuble	ventes_deco
999	             50	            20
845	             400	        60
980	             NULL	        12

|********************************************************************************|
      CREATE MYSQL TABLE & INSERT DATA
|********************************************************************************|
CREATE TABLE IF NOT EXISTS PRODUCT_NOMENCLATURE (
    product_id INT  PRIMARY KEY,
    product_type VARCHAR(10) NOT NULL,
    product_name VARCHAR(64) NOT NULL
);

insert into PRODUCT_NOMENCLATURE(product_id,product_type,product_name) values(490756,'MEUBLE', 'Chaise');
insert into PRODUCT_NOMENCLATURE(product_id,product_type,product_name) values(389728,'DECO', 'Boule');
insert into PRODUCT_NOMENCLATURE(product_id,product_type,product_name) values(549380,'MEUBLE', 'Canape');
insert into PRODUCT_NOMENCLATURE(product_id,product_type,product_name) values(293718,'DECO', 'Mug');



CREATE TABLE IF NOT EXISTS TRANSACTION (
    date DATE NOT NULL,
    order_id INT NOT NULL,
    client_id INT NOT NULL,
    prod_id INT NOT NULL,
    prod_price DOUBLE NOT NULL,
    prod_qty INT NOT NULL,
    PRIMARY KEY (order_id, prod_id),
    FOREIGN KEY (prod_id)
        REFERENCES PRODUCT_NOMENCLATURE (product_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
);

insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-01" AS DATE),1234,999,490756,50,1);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-01" AS DATE),1234,999,389728,5,4);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,490756,50,2);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,549380,300,1);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,293718,10,6);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),7890,980,293718,2,6);
