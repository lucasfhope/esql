import pytest
import pandas as pd
import numpy as np
import pprint

from src.esql.main import _enforce_allowed_dtypes
from src.esql.parser.util import get_keyword_clauses, parse_select_clause, parse_where_clause
from src.esql.parser.types import ParsedSelectClause, AggregatesDict, GlobalAggregate, GroupAggregate, ParsedWhereClause, LogicalOperator, SimpleCondition, CompoundCondition, NotCondition
from src.esql.parser.error import ParsingError, ParsingErrorType

@pytest.fixture
def columns() -> dict[str, np.dtype]: 
    data = _enforce_allowed_dtypes(
        pd.read_csv(
            'public/data/sales.csv'
        )
    )
    columns = data.dtypes.to_dict()
    return columns


###########################################################################
# GET_KEYWORD_CLAUSES TESTS
###########################################################################
def test_get_keyword_clauses_splits_by_keywords_properly():
    keyword_clauses = get_keyword_clauses(
        query="SELECT cust, prod, date OVER bad, good, better, best WHERE quant > 10 and credit SUCH THAT bad.month = 7 and good.month = 8 and better.month = 9 and best.month = 10 having good.quant.sum > 100 and bad.quant.sum < 100 order by 2".lower()
    )
    expected = {
        "SELECT": "cust, prod, date",
        "OVER": "bad, good, better, best",
        "WHERE": "quant > 10 and credit",
        "SUCH THAT": "bad.month = 7 and good.month = 8 and better.month = 9 and best.month = 10",
        "HAVING": "good.quant.sum > 100 and bad.quant.sum < 100",
        "ORDER BY": "2"
    }
    assert keyword_clauses == expected

def test_get_keyword_clauses_splits_properly_with_missing_keywords():
    keyword_clauses = get_keyword_clauses(
        query="SELECT cust, prod, date OVER bad, good, better, best SUCH THAT bad.month = 7 and good.month = 8 and better.month = 9 and best.month = 10 HAVING good.quant.sum > 100 and bad.quant.sum < 100".lower()
    )
    expected = {
        "SELECT": "cust, prod, date",
        "OVER": "bad, good, better, best",
        "WHERE": "",
        "SUCH THAT": "bad.month = 7 and good.month = 8 and better.month = 9 and best.month = 10",
        "HAVING": "good.quant.sum > 100 and bad.quant.sum < 100",
        "ORDER BY": ""
    }
    assert keyword_clauses == expected

def test_get_keyword_clauses_raises_missing_select_error():
    with pytest.raises(ParsingError) as parsingError:
        get_keyword_clauses(
            query="OVER bad, good, better, best WHERE quant > 10 and credit SUCH THAT bad.month = 7 and good.month = 8 and better.month = 9 and best.month = 10 having good.quant.sum > 100 and bad.quant.sum < 100 and better order by 2".lower()
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE

def test_get_keyword_clauses_raises_missing_clause_error():
    with pytest.raises(ParsingError) as parsingError:
        get_keyword_clauses(
            query="SELECT cust OVER bad, good WHERE quant > 10 SUCH THAT  HAVING good.quant.sum > 100 and bad.quant.sum < 100 and better order by 2".lower()
        )
    assert parsingError.value.error_type == ParsingErrorType.MISSING_CLAUSE and "SUCH THAT" in parsingError.value.message

def test_get_keyword_clauses_raises_clause_order_error():
    with pytest.raises(ParsingError) as parsingError:
        get_keyword_clauses(
            query="SELECT cust WHERE quant > 10 OVER bad, good  SUCH THAT  HAVING good.quant.sum > 100 and bad.quant.sum < 100 and better order by 2".lower()
        )
    assert parsingError.value.error_type == ParsingErrorType.CLAUSE_ORDER and "WHERE" in parsingError.value.message

    
###########################################################################
# PARSE_SELECT_CLAUSE TESTS
###########################################################################
def test_parse_select_clause_returns_expected_structure(columns: dict[str, np.dtype]):
    parsedSelectClause = parse_select_clause(
        select_clause="cust, prod, date, quant.sum, 1.quant.max, 2.quant.min, 3.quant.avg, 3.month.count",
        groups=['1','2','3'],
        columns=columns
    )
    expected = ParsedSelectClause(
        columns=['cust', 'prod', 'date'],
        aggregates=AggregatesDict(
            global_scope=[
                GlobalAggregate(column='quant', function='sum', datatype=float),
            ],
            group_specific=[
                GroupAggregate(column='quant', function='max', datatype=float, group='1'),
                GroupAggregate(column='quant', function='min', datatype=float, group='2'),
                GroupAggregate(column='quant', function='avg', datatype=float, group='3'),
                GroupAggregate(column='month', function='count', datatype=float, group='3'),
            ]
        )
    )
    assert parsedSelectClause == expected

def test_parse_select_clause_finds_invalid_aggregate_group(columns: dict[str, np.dtype]):
    with pytest.raises(ParsingError) as parsingError:
        parse_select_clause(
            select_clause="cust, prod, date, quant.sum, 1.quant.max, 2.quant.min, 3.quant.avg",
            groups=['1','2'],
            columns=columns
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE and "Invalid aggregate group: '3.quant.avg'" in parsingError.value.message

def test_parse_select_clause_finds_invalid_aggregate_column(columns: dict[str, np.dtype]):
    with pytest.raises(ParsingError) as parsingError:
        parse_select_clause(
            select_clause="cust, prod, date, quant.sum, 1.quant.max, 2.q.min, 3.quant.avg",
            groups=['1','2','3'],
            columns=columns
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE and "Invalid aggregate column: '2.q.min'" in parsingError.value.message

def test_parse_select_clause_finds_aggregate_with_non_numeric_column(columns: dict[str, np.dtype]):
    with pytest.raises(ParsingError) as parsingError:
        parse_select_clause(
            select_clause="cust, prod, date, quant.sum, 1.quant.max, 2.credit.min, 3.quant.avg",
            groups=['1','2','3'],
            columns=columns
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE and "Invalid aggregate. Column is not a numeric type: '2.credit.min'" in parsingError.value.message

def test_parse_select_clause_finds_invalid_aggregate_function(columns: dict[str, np.dtype]):
    with pytest.raises(ParsingError) as parsingError:
        parse_select_clause(
            select_clause="cust, prod, date, quant.mean, 1.quant.max, 3.quant.avg",
            groups=['1','2','3'],
            columns=columns
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE and "Invalid aggregate function: 'quant.mean'" in parsingError.value.message

def test_parse_select_clause_finds_invalid_column(columns: dict[str, np.dtype]):
    with pytest.raises(ParsingError) as parsingError:
        parse_select_clause(
            select_clause="cust_, prod, date, 1.quant.max, 3.quant.avg",
            groups=['1','2','3'],
            columns=columns
        )
    assert parsingError.value.error_type == ParsingErrorType.SELECT_CLAUSE and "Invalid column: 'cust_'" in parsingError.value.message


###########################################################################
# PARSE_WHERE_CLAUSE TESTS
###########################################################################
def test_parse_where_clause_returns_expected_structure(columns: dict[str, np.dtype]):
    parsedWhereClause = parse_where_clause(
        where_clause="not (cust = 'Dan') and (month = 7 or month = 8 and year = 2020) and credit",
        columns=columns
    )
    expected: ParsedWhereClause = CompoundCondition(
        operator=LogicalOperator.AND,
        conditions=[
            NotCondition(
                operator=LogicalOperator.NOT,
                condition=SimpleCondition(
                    column='cust',
                    operator='=',
                    value='Dan'
                )
            ),
            CompoundCondition(
                operator=LogicalOperator.OR,
                conditions=[
                    SimpleCondition(
                        column='month',
                        operator='=',
                        value=7.0
                    ),
                    CompoundCondition(
                        operator=LogicalOperator.AND,
                        conditions=[
                            SimpleCondition(
                                column='month',
                                operator='=',
                                value=8.0
                            ),
                            SimpleCondition(
                                column='year',
                                operator='=',
                                value=2020.0
                            )
                        ]
                    )
                ]
            ),
            SimpleCondition(
                column='credit',
                operator='=',
                value=True
            )
        ]
    )
    assert parsedWhereClause == expected
        



