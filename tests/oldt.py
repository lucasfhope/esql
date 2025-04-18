import pandas as pd
#import pytest

from src.esql.parser.parse import get_processed_query, ParsingError

'''
HELPER FUNCTIONS
'''

@pytest.fixture
def sales_data() -> pd.DataFrame: 
    data = pd.read_csv('public/data/sales.csv')
    return data

def _test_valid_queries(valid_queries: list[str], data: pd.DataFrame, test_focus: str) -> None:
    for query in valid_queries:
        try:
            get_processed_query(data, query)
        except ParsingError as e:
            pytest.fail(f"{test_focus} \n\n'{query}'\n\n failed with error: {e}")

def _test_invalid_queries(invalid_queries: list[str], data: pd.DataFrame, test_focus: str) -> None:
    for query in invalid_queries:
        try:
            get_processed_query(data, query)
        except ParsingError:
            continue
        except Exception:
            print(f"{test_focus} unexpected error \n\n'{query}'\n") 
            raise
        print(f"{test_focus} failed to raise ParsingError \n\n'{query}'")


'''
PARSING ERROR DETECTION TESTS
'''

def test_valid_select_clauses(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries= [
            "SELECT cust, quant.sum OVER g1 WHERE quant > 100",
            "SELECT prod, quant.avg OVER g1 WHERE state = 'NY'",
            "SELECT cust, quant.min, quant.max OVER g1,g2 WHERE credit = true",
            "SELECT state, g1.quant.sum OVER g1 WHERE year = 2020",
            "SELECT prod, g1.quant.count, g2.quant.avg OVER g1,g2 WHERE month >= 6"
        ],
        data=sales_data,
        test_focus="SELECT CLAUSE"
    )

def test_invalid_select_clauses(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries= [
            "SELECT invalid_col OVER g1",
            "SELECT OVER g1",
            "SELECT prod, quant.invalid OVER g1",
            "SELECT prod, invalid.sum OVER g1",
            "SELECT g3.quant.sum OVER g1,g2"
        ],
        data=sales_data,
        test_focus="SELECT CLAUSE"
    )

def test_invalid_over_clauses(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries= [
            "SELECT cust OVER g 1 WHERE quant > 100",
            "SELECT prod OVER g1, WHERE state = 'NY' AND credit = true",
            "SELECT state OVER g1, long name WHERE year = 2020 AND month <= 6",
        ],
        data=sales_data,
        test_focus="OVER CLAUSE"
    )

def test_valid_where_clauses(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries = [
            "SELECT cust OVER g1 WHERE quant > 100",
            "SELECT prod OVER g1 WHERE state = 'NY' AND credit = true",
            "SELECT state OVER g1 WHERE year = 2020 AND (month <= 6 or month = 12)",
            "SELECT cust OVER g1 WHERE NOT quant >= 500 AND state = 'CT' or credit = true",
            "SELECT prod OVER g1 WHERE day < 15 AND month = 7 AND year = 2018",
            "SELECT cust OVER g1 WHERE NOT quant > 100",
            "SELECT prod OVER g1 WHERE NOT state = 'NY'",
            "SELECT state OVER g1 WHERE NOT (year = 2020 AND month <= 6)",
            "SELECT cust OVER g1 WHERE NOT (quant >= 500 AND state = 'CT')"
        ],
        data=sales_data,
        test_focus="WHERE CLAUSE"
    )

def test_invalid_where_clauses(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries = [
            "SELECT cust OVER g1 WHERE quant >>= 100",
            "SELECT prod OVER g1 WHERE state = ",
            "SELECT state OVER g1 WHERE AND year = 2020",
            "SELECT cust OVER g1 WHERE quant > 500 OR OR state = 'CT'",
            "SELECT prod OVER g1 WHERE = 'Apple'",
            "SELECT cust OVER g1 WHERE quant > 100 AND AND state = 'NY'",
            "SELECT prod OVER g1 WHERE state = 'NY' OR OR credit = true"
        ],
        data=sales_data,
        test_focus="WHERE CLAUSE"
    )

def test_valid_such_that_clauses(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries = [
            "SELECT cust OVER g1 SUCH THAT g1.quant > 100",
            "SELECT prod, state OVER g1,g2 SUCH THAT not g1.quant > 50, g2.credit = true",
            "SELECT cust OVER g1 SUCH THAT g1.year = 2020 AND g1.quant > 80",
            "SELECT prod OVER g1 SUCH THAT g1.day < 15 OR g1.state = 'NY'",
            "SELECT state OVER g1,g2,g3 SUCH THAT g1.credit = true AND g1.quant > 300, g2.prod = 'Apple', g3.cust = 'Dan' or g3.cust = 'Sam'",
            "SELECT prod OVER g1 SUCH THAT g1.day < 15 AND not (g1.state = 'NY' OR g1.year = 2021)",
            "SELECT cust OVER g1 SUCH THAT NOT g1.quant > 50",
            "SELECT prod OVER g2 SUCH THAT NOT g2.credit = true",
            "SELECT state OVER g1 SUCH THAT NOT (g1.year = 2020 AND g1.quant > 80)"
        ],
        data=sales_data,
        test_focus="SUCH THAT CLAUSE"
    )

def test_invalid_such_that_clauses(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries= [
            "SELECT cust  OVER g1 SUCH THAT quant > 100", 
            "SELECT prod  OVER g1 SUCH THAT invalid.quant > 50",
            "SELECT cust  OVER g1,g2 SUCH THAT g1.quant > 100, g2.quant < 50, g1.state = 'NY'",
            "SELECT prod  OVER g1,g2 SUCH THAT g1.year = 2020 AND AND g2.sales < 1000",
            "SELECT prod  OVER g1,g2 SUCH THAT g1.year = 2020 OR OR g2.sales < 1000" 
        ],
        data=sales_data,
        test_focus="SUCH THAT CLAUSE"
    )

def test_valid_having_clauses(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries = [
            "SELECT cust OVER g1 HAVING g1.quant.sum > 1000",
            "SELECT prod OVER g1 HAVING g1.quant.avg >= 50 AND g1.quant.count > 10",
            "SELECT state OVER g1 HAVING (g1.quant.max < 1000 and g1.quant.avg > 50) OR g1.quant.min > 100",
            "SELECT prod OVER g1,g2 HAVING g1.quant.sum > 5000 AND g2.quant.avg < 100",
            "SELECT cust OVER g1 HAVING g1.quant.sum > 1000 OR g1.quant.avg > 50",
            "SELECT prod OVER g1 HAVING g1.quant.avg >= 50 OR g1.quant.count > 10",
            "SELECT state OVER g1 HAVING g1.quant.max < 1000 OR g1.quant.min > 100",
            "SELECT prod OVER g1,g2 HAVING g1.quant.sum > 5000 OR g2.quant.avg < 100",
            "SELECT cust OVER g1 HAVING NOT g1.quant.sum > 1000",
            "SELECT prod OVER g1 HAVING NOT (g1.quant.avg >= 50 AND g1.quant.count > 10)"
        ],
        data=sales_data,
        test_focus="HAVING CLAUSE"
    )


def test_invalid_having_clauses(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries = [
            "SELECT cust OVER g1 HAVING quant > 1000", 
            "SELECT prod OVER g1 HAVING quant.invalid > 50",
            "SELECT state OVER g1 HAVING g1.quant.sum",
            "SELECT cust OVER g1 HAVING g3.quant.sum > 1000",
            "SELECT prod OVER g1 HAVING invalid.sum > 100",
            "SELECT prod OVER g1 HAVING g1.quant.sum > 1000 AND AND g1.quant.avg > 50",
            "SELECT cust OVER g1 HAVING g1.quant.sum > 1000 OR OR g1.quant.avg > 50"
        ],
        data=sales_data,
        test_focus="HAVING CLAUSE"
    )

def test_valid_order_by_clause(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries = [
            "SELECT cust, prod OVER g1 ORDER BY 1",
            "SELECT prod, state, quant OVER g1 ORDER BY 2",
            "SELECT cust, quant.sum OVER g1 ORDER BY 1",
            "SELECT prod, state, quant.avg OVER g1 ORDER BY 3",
            "SELECT state, quant.count, quant.max OVER g1 ORDER BY 2"
        ],
        data=sales_data,
        test_focus="ORDER BY CLAUSE"
    )

def test_invalid_order_by_clause(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries = [
            "SELECT cust OVER g1 ORDER BY 0",
            "SELECT prod OVER g1 ORDER BY 2",
            "SELECT state OVER g1 ORDER BY prod",
            "SELECT cust OVER g1 ORDER BY 1.5", 
            "SELECT prod OVER g1 ORDER BY" 
        ],
        data=sales_data,
        test_focus="ORDER BY CLAUSE"
    )

def test_valid_compound_queries(sales_data: pd.DataFrame) -> None:
    _test_valid_queries(
        valid_queries = [
            "SELECT cust, g1.quant.sum, g2.quant.max OVER g1,g2 WHERE state = 'NY' SUCH THAT g1.quant > 100, g2.prod = 'Apple' HAVING g2.quant.sum > 1000 ORDER BY 2",
            "SELECT prod, quant.avg, g1.quant.avg OVER g1 WHERE credit = true SUCH THAT g1.year = 2020 HAVING g1.quant.count > 5 order by 1",
            "SELECT state, quant.max, g1.quant.max OVER g1,g2 WHERE month >= 6 SUCH THAT g1.quant < 500, g2.quant > 10 and (g2.state = 'NY' or g2.state = 'CT') ORDER BY 1",
            "SELECT cust, g1.quant.avg OVER g1,g2 WHERE day < 15 SUCH THAT g1.state = 'CT', g2.quant > 200 HAVING g1.quant.avg > 50",
            "SELECT prod, quant.min WHERE year = 2018 HAVING quant.sum < 1000"
        ],
        data=sales_data,
        test_focus="COMPOUND QUERIES"
    )

def test_invalid_clause_order(sales_data: pd.DataFrame) -> None:
    _test_invalid_queries(
        invalid_queries=[
            "SELECT cust WHERE state = 'NY' OVER g1",
            "OVER g1 SELECT cust FROM sales",
            "SELECT prod WHERE credit = true HAVING sum_quant > 1000 SUCH THAT g1.quant > 100",
            "SELECT state OVER g1 ORDER BY 1 HAVING max_quant > 500",
            "WHERE year = 2020 SELECT cust OVER g1" 
        ],
        data=sales_data,
        test_focus="QUERY ORDER"
    )


'''
QUERY STRUCTURE TESTS
'''

def test_parsed_select_structure(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = "SELECT cust, prod, date.count, quant.sum, g1.quant.max, g1.quant.min, g2.quant.avg FROM sales OVER g1,g2"
    )
    assert result['select_clause'] == {
        'columns': ['cust', 'prod'],
        'aggregates': {
            'global': [
                {'column': 'date', 'function': 'count', 'datatype': 'numerical'},
                {'column': 'quant', 'function': 'sum', 'datatype': 'numerical'}
            ],
            'group_specific': [
                {'group': 'g1', 'column': 'quant', 'function': 'max', 'datatype': 'numerical'},
                {'group': 'g1', 'column': 'quant', 'function': 'min', 'datatype': 'numerical'},
                {'group': 'g2', 'column': 'quant', 'function': 'avg', 'datatype': 'numerical'}
            ]
        }
    }
    assert result['aggregate_groups'] == ['g1', 'g2']


def test_parsed_where_structure(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = "SELECT cust FROM sales OVER g1 WHERE NOT (quant > 100 AND state = 'NY') OR (year = 2021 AND credit = false)"
    )    
    assert result['where_conditions'] == {
        'operator': 'OR',
        'conditions': [
            {
                'operator': 'NOT',
                'condition': {
                    'operator': 'AND',
                    'conditions': [
                        {'column': 'quant', 'operator': '>', 'value': 100.0},
                        {'column': 'state', 'operator': '=', 'value': 'NY'}
                    ]
                }
            },
            {
                'operator': 'AND',
                'conditions': [
                    {'column': 'year', 'operator': '=', 'value': 2021},
                    {'column': 'credit', 'operator': '=', 'value': False}
                ]
            }
        ]
    }


def test_parsed_such_that_structure(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = "SELECT cust FROM sales OVER g1,g2 SUCH THAT g1.quant > 50 AND NOT (g1.state = 'NY' OR g1.year = 2021), g2.credit = true OR g2.credit = false"
    )    
    assert result['such_that_conditions'] == [
        {
            'group': 'g1',
            'operator': 'AND',
            'conditions': [
                {'group': 'g1', 'column': 'quant', 'operator': '>', 'value': 50.0},
                {
                    'operator': 'NOT',
                    'condition': {
                        'group': 'g1',
                        'operator': 'OR',
                        'conditions': [
                            {'group': 'g1', 'column': 'state', 'operator': '=', 'value': 'NY'},
                            {'group': 'g1', 'column': 'year', 'operator': '=', 'value': 2021}
                        ]
                    }
                }
            ]
        },
        {
            'group': 'g2',
            'operator': 'OR',
            'conditions': [
                {'group': 'g2', 'column': 'credit', 'operator': '=', 'value': True},
                {'group': 'g2', 'column': 'credit', 'operator': '=', 'value': False}
            ]
        }
    ]


def test_parsed_having_structure(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = "SELECT cust FROM sales OVER g1 HAVING NOT (g1.quant.sum > 1000 OR g1.quant.avg > 50) AND NOT (quant.max < 90 OR quant.min > 10)"
    )
    assert result['having_conditions'] == {
        'operator': 'AND',
        'conditions': [
            {
                'operator': 'NOT',
                'condition': {
                    'operator': 'OR',
                    'conditions': [
                        {'group': 'g1', 'column': 'quant', 'function': 'sum', 'operator': '>', 'value': 1000.0},
                        {'group': 'g1', 'column': 'quant', 'function': 'avg', 'operator': '>', 'value': 50.0}
                    ]
                }
            },
            {
                'operator': 'NOT',
                'condition': {
                    'operator': 'OR',
                    'conditions': [
                        {'column': 'quant', 'function': 'max', 'operator': '<', 'value': 90.0},
                        {'column': 'quant', 'function': 'min', 'operator': '>', 'value': 10.0}
                    ]
                }
            }
        ]
    }


'''
AGGREGATE FUNCTION EXTRACTION TESTS
'''

def test_aggregate_functions_in_select_only(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = """
            SELECT cust, quant.sum, g1.quant.min
            FROM sales
            OVER g1
            WHERE quant > 100
            SUCH THAT g1.state = 'NY'
            HAVING quant.avg > 50
            ORDER BY 2
        """
    )
    expected_aggregates = [
        {'column': 'quant', 'function': 'sum', 'datatype': 'numerical'},
        {'group': 'g1', 'column': 'quant', 'function': 'min', 'datatype': 'numerical'},
        {'column': 'quant', 'function': 'avg', 'operator': '>', 'value': 50.0}
    ]
    result_aggs = sorted(result.get('aggregate_functions', []), key=lambda agg: (agg.get('group', ''), agg['column'], agg['function']))
    expected_aggs = sorted(expected_aggregates, key=lambda agg: (agg.get('group', ''), agg['column'], agg['function']))
    assert result_aggs == expected_aggs

def test_aggregate_functions_in_having_nested(sales_data: pd.DataFrame) -> None:
    result = get_processed_query(
        df=sales_data,
        query = """
            SELECT cust
            FROM sales
            OVER g1
            WHERE quant > 100
            SUCH THAT g1.state = 'NY'
            HAVING (g1.quant.min > 100 AND g1.quant.max < 500) OR NOT (g1.quant.avg = 200)
            ORDER BY 1
        """
    )
    expected_aggregates = [
        {'group': 'g1', 'column': 'quant', 'function': 'min', 'operator': '>', 'value': 100.0},
        {'group': 'g1', 'column': 'quant', 'function': 'max', 'operator': '<', 'value': 500.0},
        {'group': 'g1', 'column': 'quant', 'function': 'avg', 'operator': '=', 'value': 200.0}
    ]
    result_aggs = sorted(result.get('aggregate_functions', []), key=lambda agg: (agg.get('group', ''), agg['column'], agg['function']))
    expected_aggs = sorted(expected_aggregates, key=lambda agg: (agg.get('group', ''), agg['column'], agg['function']))
    assert result_aggs == expected_aggs

def test_no_aggregate_functions():
    query = """
    SELECT cust, state
    FROM sales
    OVER g1
    WHERE quant > 100
    SUCH THAT g1.state = 'NY'
    ORDER BY 1
    """
    result = get_processed_query(query)
    # There are no aggregate expressions in SELECT or HAVING.
    assert result.get('aggregate_functions', []) == []

def test_invalid_aggregate_in_select():
    query = """
    SELECT cust, unknown.sum
    FROM sales
    OVER g1
    WHERE quant > 100
    SUCH THAT g1.state = 'NY'
    HAVING quant.avg > 50
    ORDER BY 2
    """
    with pytest.raises(ParsingError):
        get_processed_query(query)


'''Testing column indexes are store correctly in the query struct'''

def test_parser_column_indexes_for_sales():
    
    query = 'SELECT cust FROM sales'


    try:
        processed = get_processed_query(query)
    except ParsingError as e:
        pytest.fail(f"Parser raised an unexpected error: {e}")

    # Build the expected column indexes mapping.
    expected_columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date', 'credit']
    expected_indexes = {col: i for i, col in enumerate(expected_columns)}

    assert 'column_indexes' in processed, "'column_indexes' key not found in the processed query struct."
    assert processed['column_indexes'] == expected_indexes, (
        f"Expected column_indexes {expected_indexes} but got {processed['column_indexes']}"
    )





    









#if __name__ == '__main__':
   # pytest.main()


