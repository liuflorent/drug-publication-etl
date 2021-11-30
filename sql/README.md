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
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-01" AS DATE),1234,999,389728,3.56,4);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,490756,50,2);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,549380,300,1);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),3456,845,293718,10,6);
insert into TRANSACTION(date,order_id,client_id,prod_id,prod_price,prod_qty) values(CAST("2020-01-02" AS DATE),7890,980,293718,2,0);

SELECT t.date, sum(t.prod_price*t.prod_qty) as ventes
FROM TRANSACTION as t
where t.date BETWEEN CAST("2020-01-01" AS DATE) AND CAST("2020-12-31" AS DATE)
GROUP BY t.date
ORDER BY t.date;