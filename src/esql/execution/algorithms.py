from datetime import datetime, date
import numpy as np

from src.esql.execution.error import RuntimeError

from src.esql.parser.util import find_group_in_such_that_section
from src.esql.parser.types import ParsedSelectClause, ParsedWhereClause, ParsedSuchThatClause, ParsedHavingClause, AggregatesDict, LogicalOperator

from src.esq.execution.groupedRow import GroupedRow

def build_group_table(parsed_select_clause: ParsedSelectClause, groups: list[str] | None, parsed_where_clause: ParsedWhereClause | None, parsed_such_that_clause: ParsedSuchThatClause, parsed_having_clause: ParsedHavaingClause, aggregates: AggregatesDict, datatable: list[list[int | str | bool | date]], column_dtypes: dict[str, np.dtype], column_indices: dict[str, int]):
    grouping_attributes = parsed_select_clause['grouping_attributes']
    global_aggregates = aggregates['global_scope']
    group_aggregates = aggregates['group_specific']

    filtered_datatable = datatable
    if parsed_where_clause:
        filtered_datatable = [datatable_row for datatable_row in datatable 
            if _evaluate_condition(
                condition=parsed_where_clause,
                row=datatable_row,
                column_indices=column_indices
            )
        ]

    grouped_table = {}
    for datatable_row in filtered_datatable:
        grouping_attribute_combination = tuple(datatable_row[column_indices[attribute]] for attribute in grouping_attributes)
        if grouping_attribute_combination in grouped_table:
            grouped_row = grouped_table.get(grouping_attribute_combination)
            for aggregate in global_aggregates:
                grouped_row.update_data_map(aggregate, datatable_row)
        else:
            grouped_row = GroupedRow(grouping_attributes, aggregates, datatable_row)
            grouped_table[grouping_attribute_combination] = grouped_row 
    
    if parsed_such_that_clause: 
        for group in groups:
            group_such_that_section = next(
                (such_that_section 
                for such_that_section in parsed_such_that_clause 
                if find_group_in_such_that_section(such_that_section) == group),
                None
            )
            if not group_such_that_section:
                continue
            for datatable_row in filtered_datatable:
                if _evaluate_condition(
                    condition=group_such_that_section,
                    row=datatable_row,
                    column_indices=column_indices
                ):
                    grouping_attribute_combination = tuple(datatable_row[column_indices[attribute]] for attribute in grouping_attributes)
                    grouped_row = grouped_table.get(grouping_attribute_combination)
                    if grouped_row:
                        for aggregate in group_aggregates:
                            if aggregate['group'] == group:
                                grouped_row.update_data_map(aggregate, datatable_row)

    grouped_table_list = list(grouped_table.values())

    for grouped_row in grouped_table_list:
        grouped_row.convert_avg_in_data_map()
    
    if having_conditions:
        grouped_table_list = [grouped_row for grouped_row in grouped_table_list
            if evaluate_having_clause(
                having_condition=parsed_having_clause,
                data_map=grouped_row.data_map
            )
        ]
    return grouped_table_list


###############################################################################
# Evaluation
###############################################################################
def _evaluate_having_clause(condition: ParsedHavingClause, data_map: dict[str, str | int | bool | date]):
    operator = condition.get('operator')
    if operator == LogicalOperator.NOT:
        return not evaluate_having_clause(condition.get("condition"), data_map)

    if 'conditions' in condition:
        if operator == LogicalOperator.AND:
            return all(evaluate_having_clause(sub, data_map) for sub in condition["conditions"])
        elif operator == LogicalOperator.OR:
            return any(evaluate_having_clause(sub, data_map) for sub in condition["conditions"])
        else:
            raise RuntimeError(f"Unknown logical operator in HAVING clause: '{operator}'")

    if 'function' in condition:
        if "group" in condition:
            aggregate_key = f"{condition['group']}.{condition['column']}.{condition['function']}"
        else:
            aggregate_key = f"{condition['column']}.{condition['function']}"
    else:
        raise RuntimeError(f"Could not recognize the condition in the HAVING clause: '{condition}'")
    
    return _evaluate_actual_vs_expected_value(
        actual_value=data_map.get(aggregate_key),
        condition_value=condition.get('value')
    )


def _evaluate_condition(condition: dict, datatable_row: list, column_indices: dict[str, int]):
    if 'column' in condition:
        column = condition.get('column')
        operator = condition.get('operator')
        condition_value = condition.get('value')
        column_index = column_indices.get(column)
        if not index:
            raise RuntimeError(f"Column '{column}' not found in datatable")
        actual_value = row[index]
        return _evaluate_actual_vs_expected_value(
            actual_value=data_map.get(aggregate_key),
            condition_value=condition.get('value')
        )
    
    operator = condition.get('operator')
    if operator == LogicalOperator.AND:
        return all(evaluate_condition(simple_condition, datatable_row) for simple_condition in condition.get('conditions', []))
    elif op == LogicalOperator.OR:
        return any(evaluate_condition(simple_condition, datatable_row) for simple_conditio in condition.get('conditions', []))
    elif op == LogicalOperator.NOT:
        return not evaluate_condition(condition.get('condition'), datatable_row)
    else:
        raise RuntimeError(f"Unknown logical operator: {operator}")

    raise RuntimeError(f"Condition could not be evaluated. Expected different structure from parser:\n\n{condition}")


def _evaluate_actual_vs_expected_value(actual_value: str | int | bool | date, condition_value: str | int | bool | date) -> bool:
    if operator in ['=', '==']:
        return actual_value == condition_value
    elif operator == '>':
        return actual_value > condition_value
    elif operator == '<':
        return actual_value < condition_value
    elif operator == '>=':
        return actual_value >= condition_value
    elif operator == '<=':
        return actual_value <= condition_value
    elif operator == '!=':
        return actual_value != condition_value
    else:
        raise RuntimeError(f"Unknown operator in condition: '{condition}'")



###############################################################################
# Projection and Ordering
###############################################################################
def project_select_attributes(grouped_table_list: list[GroupedRow]) -> list[dict[str, str | int | bool | date]]:
    # THIS WILL NOT BE IN THE SAME ORDER AS WAS GIVEN IN THE SELECT CLAUSE
    grouping_attributes_and_aggregate_keys = grouped_table_list[0].data_map.keys()
    select_table = []
    for grouped_row in grouped_table_list:
        row = {}
        for column_name in grouping_attributes_and_aggregate_keys:
            row[column_name] = grouped_row.data_map.get(column_name)
        select_table.append(row)
    return select_table

def order_by_sort(select_table: list[dict[str, str | int | bool | date]], order_by: int, grouping_attributes: list[str]):
    if order_by > 0:
        column_sort_keys = tuple(grouping_attributes[:order_by])
        select_table.sort(key=lambda row: tuple(row.get(column_name) for column_name in column_sort_keys))
    return select_table






