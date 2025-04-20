import pandas as pd

from src.esql.parser.types import ParsedQuery
from src.esql.execution import algorithms


def execute(parsed_query: ParsedQuery) -> pd.DataFrame:
    pd_datatable = parsed_query['data']
    column_dtypes = pd_datatable.dtypes.to_dict()
    columns = pd_datatable.columns.tolist()
    column_indices = { column: index for index, column in enumerate(columns) }
    datatable = pd_datatable.values.tolist()

    grouped_table = algorithms.build_grouped_table(
        parsed_select_clause=parsed_query['select'], 
        groups=parsed_query['over'], 
        parsed_where_clause=parsed_query['where'], 
        parsed_such_that_clause=parsed_query['such_that'], 
        parsed_having_clause=parsed_query['having'], 
        aggregates=parsed_query['aggregates'], 
        datatable=datatable, 
        column_indices=column_indices
    )
    
    projected_table = algorithms.project_select_attributes(
        parsed_select_clause=parsed_query['select'],
        grouped_table=grouped_table
    )
    
    ordered_table = algorithms.order_by_sort(
        projected_table=projected_table,
        order_by=parsed_query['order_by'],
        grouping_attributes=parsed_query['select']['grouping_attributes']
    )
    
    return pd.DataFrame(ordered_table)