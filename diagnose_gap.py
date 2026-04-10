from dotenv import load_dotenv
load_dotenv()
from core.supabase_client import execute_sql

r1 = execute_sql("SELECT COUNT(*) as c, MAX(order_date) as latest FROM orders WHERE order_status IN ('Delivered','Shipped')")
print("All Delivered/Shipped:", r1['rows'])

r2 = execute_sql("SELECT COUNT(*) as c FROM orders WHERE order_status IN ('Delivered','Shipped') AND order_date >= '2025-10-10'")
print("Delivered/Shipped since Oct 2025:", r2['rows'])

r3 = execute_sql("SELECT COALESCE(SUM(oi.item_price),0) AS gmv FROM orders o JOIN order_items oi ON o.order_id=oi.order_id WHERE o.order_status IN ('Delivered','Shipped') AND o.order_date >= '2025-10-10'")
print("GMV since Oct 2025:", r3['rows'])

r4 = execute_sql("SELECT COALESCE(SUM(oi.item_price),0) AS gmv FROM orders o JOIN order_items oi ON o.order_id=oi.order_id WHERE o.order_status IN ('Delivered','Shipped')")
print("Total GMV all time:", r4['rows'])

print("\nDB current_date:", execute_sql("SELECT current_date")['rows'])
print("Data max date:", execute_sql("SELECT MAX(order_date) FROM orders")['rows'])
print("Gap in months:", execute_sql("SELECT EXTRACT(MONTH FROM AGE(current_date, MAX(order_date))) AS gap_months FROM orders")['rows'])
