import re
import pandas as pd

from src.esql.parser.types import ParsedQuery
from src.esql.parser.util import get_keyword_clauses, parse_over_clause, parse_select_clause, parse_where_clause, parse_such_that_clause, parse_having_clause, parse_order_by_clause


def get_parsed_query(data: pd.DataFrame, query: str) -> ParsedQuery:
    prepared_query = _prepare_query(query)
    return _build_parsed_query(
        data=data, 
        query=prepared_query
    )
    

def _prepare_query(query: str) -> str:
    # Find and separate quoted strings.
    pattern = r'"[^"]*"|\'[^\']*\''
    quoted_texts = re.findall(pattern, query)
    parts = re.split(f'({pattern})', query)

    # Lowercase all parts that are not within quotes.
    processed_parts = []
    quoted_index = 0
    for part in parts:
        if re.match(pattern, part):
            processed_parts.append(f'QUOTED{quoted_index}')
            quoted_index += 1
        else:
            processed_parts.append(part.lower())

    # Reinsert the quoted texts.
    query = ''.join(processed_parts)
    for i, qt in enumerate(quoted_texts):
        query = query.replace(f'QUOTED{i}', qt, 1)

    return ' '.join(query.split())


def _build_parsed_query(data: pd.DataFrame, query: str) -> ParsedQuery:
    keyword_clauses = get_keyword_clauses(query)
    parsed_over_clause = parse_over_clause(over_clause=keyword_clauses["OVER"])
    
    parsed_select_clause = parse_select_clause(
        select_clause=keyword_clauses["SELECT"],
        groups=parsed_over_clause,
        columns=data.columns
    )
    
    parsed_where_clause = parse_where_clause(
        where_clause=keyword_clauses["WHERE"],
        groups=parsed_over_clause,
        columns=data.columns
    )

    parsed_such_that_clauses = []
    such_that_conditions = keyword_clauses["SUCH THAT"].split(',')
    for condition in conditions:
        parsed_such_that_clauses.append(
            parse_such_that_clause(
                such_that_clause=condition,
                groups=parsed_over_clause,
                columns=data.columns
            )
        )
    
    (having_clause, aggregates) = parse_having_clause(
        having_clause=keyword_clauses["HAVING"],
        groups=parsed_over_clause,
        columns=data.columns
    )

    order_by_clause = parse_order_by_clause(
        order_by_clause=keyword_clauses["ORDER BY"],
        number_of_select_columns=len(parsed_select_clause['columns']) 
    )

    aggregates.update(parsed_select_clause['aggregates'])
    
    return ParsedQuery(
        data=data,
        select=parsed_select_clause,
        over=parsed_over_clause,
        where=parsed_where_clause,
        such_that=parsed_such_that_clauses,
        having=having_clause,
        order_by=order_by_clause,
        aggregates=aggregates
    )