import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

def update_product_stock():
    # Baca file CSV
    with open('/product.csv', 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
    
    # Update stok produk di database
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    for row in rows:
        sku = row['sku']
        stock = int(row['stock'])
        
        # Periksa apakah stok produk bertambah atau berkurang
        if stock > 0:
            sql = f"UPDATE products SET stock = stock + {stock} WHERE sku = '{sku}'"
        else:
            sql = f"UPDATE products SET stock = stock - {abs(stock)} WHERE sku = '{sku}'"
        
        cursor.execute(sql)
        conn.commit()
    
    cursor.close()
    conn.close()

dag = DAG(
    'product_stock_monitoring',
    description='Monitoring stok produk',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 4, 1),
    catchup=False
)

t1 = PythonOperator(
    task_id='update_product_stock',
    python_callable=update_product_stock,
    dag=dag
)

t1
