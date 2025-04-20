import os
import pytest
import sqlite3
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from src.esql.accessor import ESQLAccessor, _enforce_allowed_dtypes
from tests.parser.test_parse import sales_test_data


def _execute_sql_query_through_sqlite3(sql: str) -> pd.DataFrame:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    with open('public/data/load_sales_table.sql', 'r') as sql_file:
        sql_script = sql_file.read()
        cur.executescript(sql_script)
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
    sql_df = _enforce_allowed_dtypes(_execute_sql_query_through_sqlite3(sql).round(2).reset_index(drop=True))    # .sort_values(by='cust')
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
def test_select_query(sales_test_data: pd.DataFrame):
    sql = '''
        SELECT cust, prod, day, month, year, state, quant, date, credit FROM sales 
    '''
    esql = '''
        SELECT cust, prod, day, month, year, state, quant, date, credit 
    '''
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_integer_where_query(sales_test_data: pd.DataFrame):
    sql = """
        SELECT cust, quant
        FROM sales
        WHERE quant != 100
        GROUP BY cust, quant
    """
    esql = """
        SELECT cust, quant
        WHERE quant != 100
    """
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_float_where_query(sales_test_data: pd.DataFrame):
    sql = """
        SELECT cust, quant
        FROM sales
        WHERE quant <= 55.5
        GROUP BY cust, quant
    """
    esql = """
        SELECT cust, quant
        WHERE quant <= 55.5
    """
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_boolean_where_query(sales_test_data: pd.DataFrame):
    sql = """
        SELECT cust, prod, quant, date
        FROM sales
        where credit = true
    """
    esql = """
        SELECT cust, prod, quant, date
        Where credit
    """
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_gt_date_where_query(sales_test_data: pd.DataFrame):
    sql = """
        select cust,prod, sum(quant) from sales
        where date > '2019-04-12'
        group by cust, prod
    """
    esql = """
        SELECT cust, prod, quant.sum
        where date > '2019-04-12'
    """
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_eq_date_where_query(sales_test_data: pd.DataFrame):
    sql = """
        select cust,prod, count(month) from sales
        where date = '2020-4-13'
        group by cust, prod
    """
    esql = """
        SELECT cust, prod, month.count
        where date = '2020-4-13'
    """
    _test_query(sql, esql, data=sales_test_data)

@pytest.mark.timeout(5)
def test_string_where_query(sales_test_data: pd.DataFrame):
    sql = """
        SELECT cust, prod, year
        from sales
        WHERE state = 'NY'
        GROUP BY cust, prod, year
    """
    esql = """
        SELECT cust, prod, year
        WHERE state = 'NY'
    """
    _test_query(sql, esql, data=sales_test_data)

    











if __name__ == '__main__':
    pytest.main()