1. Get all current active customers
Business Question: Who are our current customers?
  
SELECT *
FROM dim_customer_scd2
WHERE is_current = 1;

2. Get current active customers only (subscription active)
Business Question: Who currently has active subscription?

SELECT *
FROM dim_customer_scd2
WHERE is_current = 1
AND subscription_status = 'Active';

3. Total number of current customers
Business Question: Current customer base size

SELECT COUNT(*) AS total_current_customers
FROM dim_customer_scd2
WHERE is_current = 1;

4. Total customers including history
Business Question: Total records including history

SELECT COUNT(*) AS total_records
FROM dim_customer_scd2;

5. Customers who changed their plan
Business Question: Who upgraded or downgraded?

SELECT customer_id, COUNT(*) AS change_count
FROM dim_customer_scd2
GROUP BY customer_id
HAVING COUNT(*) > 1;

6. Full history of a specific customer
Business Question: Track customer lifecycle

SELECT *
FROM dim_customer_scd2
WHERE customer_id = 3
ORDER BY effective_start_date;

7. Current plan distribution
Business Question: Most popular plan

SELECT plan, COUNT(*) AS total_customers
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY plan
ORDER BY total_customers DESC;

8. Customer distribution by city
Business Question: Which city has more customers?
  
SELECT city, COUNT(*) AS total_customers
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY city
ORDER BY total_customers DESC;

9. Monthly vs Yearly billing distribution
Business Question: Billing preference analysis

SELECT billing_cycle, COUNT(*) AS total_customers
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY billing_cycle;

10. Subscription status distribution
Business Question: Active vs Cancelled ratio

SELECT subscription_status, COUNT(*) AS total
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY subscription_status;

11. Customers who cancelled subscription
Business Question: Churn analysis

SELECT *
FROM dim_customer_scd2
WHERE is_current = 1
AND subscription_status = 'Cancelled';

12. Customers who upgraded plan (Basic → Pro)
Business Question: Upgrade tracking

SELECT customer_id
FROM dim_customer_scd2
GROUP BY customer_id
HAVING COUNT(DISTINCT plan) > 1;

13. Number of historical changes per customer
Business Question: Who changes frequently?

SELECT customer_id, COUNT(*) AS versions
FROM dim_customer_scd2
GROUP BY customer_id
ORDER BY versions DESC;

14. Customers with most recent changes
Business Question: Recently modified customers

SELECT *
FROM dim_customer_scd2
ORDER BY record_updated_ts DESC
LIMIT 10;

15. Currently active customers by source system
Business Question: Customer acquisition source

SELECT source_system, COUNT(*) AS total_customers
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY source_system;

16. Customers whose email is verified
Business Question: Verified vs non-verified customers

SELECT email_verified, COUNT(*) AS total
FROM dim_customer_scd2
WHERE is_current = 1
GROUP BY email_verified;

17. Customer churn rate (basic version)
SELECT
COUNT(CASE WHEN subscription_status = 'Cancelled' THEN 1 END) * 100.0 / COUNT(*) AS churn_rate_percentage
FROM dim_customer_scd2
WHERE is_current = 1;

18. Customer lifecycle duration
Business Question: How long customer stayed in a plan

SELECT
customer_id,
effective_start_date,
effective_end_date,
DATEDIFF(effective_end_date, effective_start_date) AS duration_days
FROM dim_customer_scd2;

19. Customers with multiple changes (frequent modifiers)
SELECT customer_id
FROM dim_customer_scd2
GROUP BY customer_id
HAVING COUNT(*) >= 3;

20. Latest record of each customer (without using is_current)

SELECT *
FROM dim_customer_scd2 d
WHERE effective_start_date =
(
    SELECT MAX(effective_start_date)
    FROM dim_customer_scd2
    WHERE customer_id = d.customer_id
);
