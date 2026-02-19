-- Day 4: E-Commerce DB Simulation

DELETE FROM orders;
DELETE FROM products;
DELETE FROM customers;

INSERT INTO customers (name, city) VALUES ('Alice', 'New York');
INSERT INTO customers (name, city) VALUES ('Bob', 'Los Angeles');
INSERT INTO customers (name, city) VALUES ('Charlie', 'Minessota');
INSERT INTO customers (name, city) VALUES ('Batler', 'Denver');
INSERT INTO customers (name, city) VALUES ('Johnny', 'New Mexico');
INSERT INTO customers (name, city) VALUES ('Clyde', 'Spartanburg');
INSERT INTO customers (name, city) VALUES ('Kendra', 'Boston');
INSERT INTO customers (name, city) VALUES ('Malice', 'Wyoming');
INSERT INTO customers (name, city) VALUES ('Helena', 'Miami');
INSERT INTO customers (name, city) VALUES ('Josh', 'Saint Louis');
-- Add 7 more (e.g., 'David', 'Miami'; vary cities for analysis)

INSERT INTO products (name, price) VALUES ('Laptop', 1200.50);
INSERT INTO products (name, price) VALUES ('Phone', 800.00);
INSERT INTO products (name, price) VALUES ('Book', 15.99);
INSERT INTO products (name, price) VALUES ('Lighter', 12.50);
INSERT INTO products (name, price) VALUES ('Solar Charger', 35.99);
INSERT INTO products (name, price) VALUES ('Bag', 88.50);
INSERT INTO products (name, price) VALUES ('Pencils', 8.99);
INSERT INTO products (name, price) VALUES ('Monitor', 350.88);
INSERT INTO products (name, price) VALUES ('Camera', 39.99);
INSERT INTO products (name, price) VALUES ('Desk', 485.88);
-- Add 7 more (e.g., 'Headphones', 100.00; mix prices)

INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (1, 1, 2, '2023-01-15');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (2, 2, 1, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (3, 3, 4, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (4, 5, 3, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (5, 4, 6, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (6, 7, 8, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (7, 6, 6, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (8, 9, 3, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (9, 3, 2, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (4, 6, 3, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (6, 7, 6, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (8, 8, 2, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (3, 5, 4, '2023-02-20');
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES (1, 7, 3, '2023-02-20');
-- Add 10+ more (vary ids, quantities, dates like '2023-03-10')