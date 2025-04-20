import os
import pytest
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from src.esql.accessor import ESQLAccessor, _enforce_allowed_dtypes
from tests.parser.test_parse import sales_test_data


def _execute_sql_query_through_postgres(sql: str) -> pd.DataFrame:
    load_dotenv()
    host = os.getenv('HOST')
    user = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')
    port = os.getenv('PORT')
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        cursor_factory=psycopg2.extras.DictCursor
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(
        [dict(row) for row in rows]
    )

def _debug_mismatched_result_sets(expected_set: set[list[int | str | bool | date]], received_set: set[list[int | str | bool | date]]) -> None:
    missing = expected_set - received_set
    extra = received_set - expected_set
    print("\n\n--- DEBUG INFO ---")
    print(f"Total expected rows: {len(expected_set)}")
    print(f"Total received rows: {len(received_set)}")
    print(f"Missing rows ({len(missing)}):", missing)
    print(f"Extra rows ({len(extra)}):", extra)
    print("--- END DEBUG ---\n\n")

def _test_query(sql: str, esql: str, data: pd.DataFrame) -> None:
    sql_df = _enforce_allowed_dtypes(_execute_sql_query_through_postgres(sql).round(2).reset_index(drop=True))    # .sort_values(by='cust')
    esql_df = data.esql.query(esql).round(2).reset_index(drop=True)
    sql_set = set(tuple(row) for row in esql_df.to_numpy())
    esql_set = set(tuple(row) for row in esql_df.to_numpy())
    if sql_set != esql_set:
        _debug_mismatched_result_sets(
            expected_set=sql_set,
            received_set=esql_set
        )
    assert sql_set == esql_set, "Queries did not return the same result."


@pytest.mark.timeout(5)
def test_simple_query(sales_test_data: pd.DataFrame):
    sql = '''
        SELECT cust, prod, day, month, year, state, quant, date, credit FROM sales 
    '''
    esql = '''
        SELECT cust, prod, day, month, year, state, quant, date, credit 
    '''
    _test_query(sql, esql, sales_test_data)
    











if __name__ == '__main__':
    pytest.main()