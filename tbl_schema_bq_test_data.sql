SELECT column_name,data_type,

CASE
WHEN data_type IN ("FLOAT64", "INT64") THEN 'ifnull'
ELSE
'string' 

END
FROM `hci-project-313508.amex_credit_card`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'amex_customers_test'
