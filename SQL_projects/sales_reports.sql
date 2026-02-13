-- Sales Reports

-- Total revenue (join + aggregate)
SELECT SUM(o.quantity * p.price) AS total_revenue FROM orders o JOIN products p ON o.product_id = p.id;

-- Top customers by spend
SELECT c.name, SUM(o.quantity * p.price) AS total_spent FROM orders o JOIN products p ON o.product_id = p.id JOIN customers c ON o.customer_id = c.id GROUP BY c.name ORDER BY total_spent DESC LIMIT 5;

-- Sales by city (multi-join)
SELECT c.city, SUM(o.quantity * p.price) AS city_revenue FROM orders o JOIN products p ON o.product_id = p.id JOIN customers c ON o.customer_id = c.id GROUP BY c.city;

-- Recent orders (filter + join)
SELECT c.name, p.name, o.quantity, o.order_date FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id WHERE o.order_date > '2023-02-01' ORDER BY o.order_date DESC;