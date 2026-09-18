"""
DAG: adventure_works_customer_rf
Description: ETL Pipeline for Customer Segmentation & RFM Analysis from AdventureWorks
           to Data Warehouse schema dwh.

Pipeline : check_connection -> extract -> transform → load
Source : Sales and Person schema (AdventureWorks)
Target : schema dwh
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

CONN_ID = "adventure_works"
TARGET_SCHEMA = "dwh"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Helper
def get_hook():
    return PostgresHook(postgres_conn_id=CONN_ID)

# Task 1 - Check Connection
def check_connection(**kwargs):
    """Connection verification to PostgreSQL adventure_works."""
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"[check_connection] Koneksi berhasil!")
    print(f"[check_connection] {version[0]}")

    cursor.close()
    conn.close()

# Task 2  - Extract
# Source Tables: SalesOrderHeader, SalesOrderDetail, Customer, SalesTerritory, Person 
# Target Table : stg_customer_orders

def extract(**kwargs):
    """
    Extract order details and customers from AdventureWorks.
    Raw dataset will be saved into stg_customer_orders
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Setup schema & tabel staging 
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

        # stg_customer_orders
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."stg_customer_orders" (
                "SalesOrderID"            INT
                , "OrderDate"             DATE
                , "CustomerID"            INT
                , "PersonID"              INT
                , "CustomerName"          VARCHAR(100)
                , "LineTotal"             NUMERIC(15, 2)
                , "TerritoryID"           INT
                , "TerritoryName"         VARCHAR(100)
                , "CountryRegionCode"     VARCHAR(10)
            );
        """)

        # Truncate before loading
        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."stg_customer_orders";')


        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."stg_customer_orders" (
                "SalesOrderID"
                , "OrderDate"
                , "CustomerID"  
                , "PersonID"   
                , "CustomerName"  
                , "LineTotal"
                , "TerritoryID"
                , "TerritoryName"
                , "CountryRegionCode"
            )

            -- CTE for joining customer information
            -- Source Tables : Sales.Customer & Person.Person
            WITH customer_cte AS (
                SELECT 
                    c."CustomerID"
                    , c."PersonID"
                    , COALESCE(p."FirstName", '') || ' ' || COALESCE(p."MiddleName", '') || ' ' || COALESCE(p."LastName", ' ') AS "CustomerName"
                FROM "Sales"."Customer" c
                LEFT JOIN "Person"."Person" p
                    ON c."PersonID" = p."BusinessEntityID"
            )


            -- CTE for calculating total spending  and aggregating total spending per salesorderid
            , sales_order_agg AS (
                SELECT
                    "SalesOrderID"
                    , SUM("OrderQty" * "UnitPrice" * (1 - "UnitPriceDiscount")) AS "LineTotal"
                FROM "Sales"."SalesOrderDetail"
                GROUP BY 1
            )

            -- CTE for joining sales information
            , sales_cte AS (
                SELECT
                    soh."SalesOrderID"
                    , soh."OrderDate"
                    , soh."CustomerID"
                    , soa."LineTotal" 
                    , soh."TerritoryID"
                    , st."Name" AS "TerritoryName"
                    , st."CountryRegionCode"
                FROM "Sales"."SalesOrderHeader" AS soh
                INNER JOIN sales_order_agg AS soa
                    ON soh."SalesOrderID" = soa."SalesOrderID"
                INNER JOIN "Sales"."SalesTerritory" AS st
                    ON soh."TerritoryID" = st."TerritoryID"
                WHERE st."Name" IN ('Northwest', 'Southwest')
            )

            -- Main Query
            SELECT
                s."SalesOrderID"
                , s."OrderDate"
                , s."CustomerID"
                , c."PersonID"
                , c."CustomerName"
                , s."LineTotal"
                , s."TerritoryID"
                , s."TerritoryName"
                , s."CountryRegionCode"
            FROM sales_cte s
            LEFT JOIN customer_cte c
                ON s."CustomerID" = c."CustomerID";
        """)
        cursor.execute(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."stg_customer_orders";')
        total_rows = cursor.fetchone()[0]

        conn.commit()
        print(f"[extract] stg_customer_orders has been extracted. There are {total_rows} rows")

    except Exception as e:
        conn.rollback()
        print(f"[extract] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

# Task 3 : Transform
def transform(**kwargs):
    """
    Calculate from dwh."stg_customer_orders" : 
      - recency : recency (the latest date when customer did transaction)
      - frequency : frequency (how many times customer did transaction )
      - monetary  : monetary (total amount spent by customer)
      - r_score : 1-4 from NTILE(4) ORDER BY Recency ASC
      - f_score : 1-4 FROM NTILE(4) ORDER BY Frequency DESC
      - m_score : 1-4 from NTILE(4) ORDER BY Monetary DESC
      - rfm_score : R_Score + F_Score + M_Score 
    Result will be saved in dwh.trf_customer_rfm
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."trf_customer_rfm" (
                "CustomerID"        INT
                , "CustomerName"    VARCHAR(100)
                , "TerritoryName"   VARCHAR(100)
                , "Recency"         INT
                , "Frequency"       INT
                , "Monetary"        NUMERIC(15,2)
                , "R_Score"         INT
                , "F_Score"         INT
                , "M_Score"         INT
                , "RFM_Score"       INT
                , "Segment"          VARCHAR(100)
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."trf_customer_rfm";')

        # Transformation : calculation
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."trf_customer_rfm" (
                "CustomerID"        
                , "CustomerName"    
                , "TerritoryName"   
                , "Recency"         
                , "Frequency"       
                , "Monetary"       
                , "R_Score"         
                , "F_Score"         
                , "M_Score"         
                , "RFM_Score"       
                , "Segment"          
            )

            -- CTE for aggregating recency, frequency, and monetary
            WITH agg AS (    
                SELECT 
                    "CustomerID"        
                    , "CustomerName"    
                    , "TerritoryName"   
                    , (SELECT MAX("OrderDate") FROM "{TARGET_SCHEMA}"."stg_customer_orders") - MAX("OrderDate") AS "Recency"
                    , COUNT(DISTINCT "SalesOrderID")  AS "Frequency"
                    , SUM("LineTotal") AS "Monetary"
                FROM  "{TARGET_SCHEMA}"."stg_customer_orders"
                GROUP BY 1,2,3
            )

            , rfm_calculation AS (
                SELECT
                    "CustomerID"        
                    , "CustomerName"    
                    , "TerritoryName"
                    , "Recency"
                    , "Frequency"
                    , "Monetary" 
                    , NTILE(4) OVER(ORDER BY "Recency" DESC) AS "R_Score"
                    , NTILE(4) OVER(ORDER BY "Frequency" ASC) AS "F_Score"
                    , NTILE(4) OVER(ORDER BY "Monetary" ASC) AS "M_Score"
                FROM agg

            )

            -- main query
            SELECT 
                "CustomerID"        
                , "CustomerName"    
                , "TerritoryName"
                , "Recency"
                , "Frequency"
                , "Monetary" 
                , "R_Score"
                , "F_Score"
                , "M_Score"
                , "R_Score" + "F_Score" + "M_Score" AS "RFM_Score"
                , CASE 
                    WHEN "R_Score" + "F_Score" + "M_Score" > 9 THEN 'Champions'
                    WHEN "R_Score" + "F_Score" + "M_Score" > 6 THEN 'Loyal'
                    WHEN "R_Score" + "F_Score" + "M_Score" > 4  THEN 'At Risk'
                    ELSE 'Lost'
                END AS "Segment"
            FROM rfm_calculation;

        """)
        cursor.execute(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."trf_customer_rfm";')
        total_rows = cursor.fetchone()[0]

        conn.commit()
        print(f"[transform] trf_customer_rfm has been created. Total {total_rows} rows")

    except Exception as e:
        conn.rollback()
        print(f"[transform] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

# Task 4 : Load
def load(**kwargs):
    """
    Load final data to dwh.fact_customer_rfm.
    Add load_timestamp
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."fact_customer_rfm" (
                "CustomerID"        INT         NOT NULL    PRIMARY KEY
                , "CustomerName"    VARCHAR(100)
                , "TerritoryName"   VARCHAR(100)
                , "Recency"         INT
                , "Frequency"       INT
                , "Monetary"        NUMERIC(15,2)
                , "R_Score"         INT
                , "F_Score"         INT
                , "M_Score"         INT
                , "RFM_Score"       INT
                , "Segment"         VARCHAR(100)
                , "LoadTimeStamp"    TIMESTAMP      DEFAULT NOW()
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."fact_customer_rfm";')

        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."fact_customer_rfm" (
                "CustomerID"        
                , "CustomerName"    
                , "TerritoryName"   
                , "Recency"         
                , "Frequency"       
                , "Monetary"       
                , "R_Score"         
                , "F_Score"         
                , "M_Score"         
                , "RFM_Score"       
                , "Segment"
                , "LoadTimeStamp"     
            )

            SELECT
                "CustomerID"        
                , "CustomerName"    
                , "TerritoryName"
                , "Recency"
                , "Frequency"
                , "Monetary" 
                , "R_Score"
                , "F_Score"
                , "M_Score"
                , "RFM_Score"
                , "Segment"
                , NOW() AS "LoadTimeStamp"     
            FROM "{TARGET_SCHEMA}"."trf_customer_rfm";
      
        """)

        cursor.execute(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."fact_customer_rfm";')
        total_rows = cursor.fetchone()[0]

        conn.commit()
        print(f"[load] Successfully load {total_rows} rows to {TARGET_SCHEMA}.fact_customer_rfm.")

    except Exception as e:
        conn.rollback()
        print(f"[load] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

# DAG definition
with DAG(
    dag_id="customer_rfm",
    default_args=default_args,
    description="ETL Customer Segmentation and RFM AdventureWorks -> dwh.fact_customer_rfm",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "adventure_works", "etl", "customer"],
) as dag:

    task_check_connection = PythonOperator(
        task_id="check_connection",
        python_callable=check_connection,
    )

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    task_check_connection >> task_extract >> task_transform >> task_load







