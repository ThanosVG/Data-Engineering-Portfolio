-- Day 5: Optimization Analysis

-- Explain a join query (shows plan—scan, search, etc.)
EXPLAIN QUERY PLAN SELECT * FROM customer_spending WHERE total_spent > 500;

-- Add another index for city
CREATE INDEX IF NOT EXISTS idx_city ON customers(city);

-- Rerun EXPLAIN to compare
EXPLAIN QUERY PLAN SELECT city, AVG(total_spent) FROM customer_spending GROUP BY city;

-- Optimized view with filter
EXPLAIN QUERY PLAN SELECT * FROM high_value_orders WHERE customer_id IN (SELECT id FROM customers WHERE city = 'New York');