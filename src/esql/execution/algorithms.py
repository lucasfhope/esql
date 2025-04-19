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


def _evaluate_having_clause(having_condition: ParsedHavingClause, data_map):
    if isinstance(condition, dict):
        op = condition.get("operator", "").upper()
        # Handle NOT operator first, regardless of whether "conditions" exists.
        if op == "NOT":
            return not evaluate_having_clause(condition.get("condition"), data_map)

        # For compound conditions with a "conditions" list:
        if "conditions" in condition:
            if op == "AND":
                return all(evaluate_having_clause(sub, data_map) for sub in condition["conditions"])
            elif op == "OR":
                return any(evaluate_having_clause(sub, data_map) for sub in condition["conditions"])
            else:
                raise Exception(f"Unknown compound operator in HAVING clause: {op}")

        # Otherwise, this is a leaf condition.
        # Build the key for an aggregate if one is referenced.
        if "function" in condition:
            if "group" in condition:
                key = f"{condition['group']}.{condition['column']}.{condition['function']}"
            else:
                key = f"{condition['column']}.{condition['function']}"
        else:
            # Assume it's a normal column condition.
            key = condition.get("column")
        
        actual = data_map.get(key)
        expected = condition.get("value")
        
        # If actual is a datetime.date and expected is a string, convert expected.
        if isinstance(actual, datetime.date) and isinstance(expected, str):
            expected = datetime.datetime.strptime(expected, '%Y-%m-%d').date()
        
        if op in ['=', '==']:
            return actual == expected
        elif op == '>':
            return actual > expected
        elif op == '<':
            return actual < expected
        elif op == '>=':
            return actual >= expected
        elif op == '<=':
            return actual <= expected
        elif op == '!=':
            return actual != expected
        else:
            raise Exception(f"Unknown operator in HAVING leaf condition: {op}")
    else:
        return bool(condition)



###############################################################################
# Projection and Ordering
###############################################################################

def project_select_attributes(hTable, select_attributes, aggregate_descriptors):
    """
    Build the final result table by selecting the specified attributes.
    
    The final result includes:
      - The grouping (non-aggregate) columns from select_attributes.
      - Aggregate columns from aggregate_descriptors. For each aggregate descriptor, the key is
        built using the same method as in H.aggregate_key(). If a key is missing in an H object's data_map,
        its value is set to None.
    
    Parameters:
        hTable (list): List of H objects.
        select_attributes (list): List of non-aggregate column names.
        aggregate_descriptors (dict): Dictionary with keys 'global' and 'group_specific',
                                      each a list of aggregate descriptor dictionaries.
    
    Returns:
        list: A list of dictionaries representing the final output rows.
    """
    agg_keys = []
    if 'global' in aggregate_descriptors:
        for agg in aggregate_descriptors['global']:
            key = f"{agg['column']}.{agg['function']}"
            agg_keys.append(key)
    if 'group_specific' in aggregate_descriptors:
        for agg in aggregate_descriptors['group_specific']:
            key = f"{agg['group']}.{agg['column']}.{agg['function']}"
            agg_keys.append(key)
    
    final_attrs = select_attributes + agg_keys
    select_table = []
    for entry in hTable:
        row = {}
        for attr in final_attrs:
            row[attr] = entry.data_map.get(attr, None)
        select_table.append(row)
    return select_table

def order_by_sort(select_table, order_by, grouping_attributes):
    """
    Sort the resulting table according to the ORDER BY clause.
    Instead of sorting on a single column, this version sorts on a tuple of the first N grouping attributes,
    where N is the value of order_by.
    
    Parameters:
        select_table (list): A list of dictionaries representing the result rows.
        order_by (int): The number of grouping attributes to sort by.
        grouping_attributes (list): The list of grouping attributes, in order.
    
    Returns:
        list: The sorted result table.
    """
    if order_by:
        sort_keys = tuple(grouping_attributes[:order_by])
        select_table.sort(key=lambda row: tuple(row.get(attr, 0) for attr in sort_keys))
    return select_table






###############################################################################
# Condition Evaluation
###############################################################################

def _evaluate_condition(condition: dict, datatable_row: list, column_indices: dict[str, int]):
    # Simple Condition
    if isinstance(condition, dict) and 'column' in condition:
        column = condition.get('column')
        operator = condition.get('operator')
        condition_value = condition.get('value')
        column_index = column_indices.get(column)
        if not index:
            raise RuntimeError(f"Column '{col}' not found in datatable")
        actual_value = row[index]
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
            raise RuntimeError(f"Unknown operator in leaf condition: {operator}")
    
    # Compound or not condition
    elif isinstance(condition, dict):
        operator = condition.get("operator").upper()
        if operator == LogicalOperator.AND:
            return all(evaluate_condition(simple_condition, datatable_row) for simple_conditions in condition.get("conditions", []))
        elif op == LogicalOperator.OR:
            return any(evaluate_condition(simple_condition, datatable_row) for simple_condition in condition.get("conditions", []))
        elif op == LogicalOperator.NOT:
            return not evaluate_condition(condition.get("condition"), datatable_row)
        else:
            raise Exception(f"Unknown compound operator: {op}")

    raise RuntimeError(f"Condition could not be evaluated. Expected different structure from parser:\n\n{condition}")


'''

# Global variables for the current datatable, column indexes, and column types.
DATATABLE = []
COLUMN_INDEXES = {}
COLUMN_TYPES = {}  # Mapping of column names to their expected types (e.g., 'date', 'string', etc.)

def normalize_column_indexes(col_indexes, expected_columns=None):
    """
    Ensure that col_indexes is a dictionary mapping column names to integer indices.
    If col_indexes is a list, convert it using the expected order.
    """
    if isinstance(col_indexes, dict):
        return col_indexes
    elif isinstance(col_indexes, list):
        if expected_columns is None:
            return {col: idx for idx, col in enumerate(col_indexes)}
        else:
            return {col: idx for idx, col in enumerate(expected_columns)}
    else:
        raise TypeError("Column indexes must be either a dict or a list.")

def set_datatable_information(datatable, col_indexes, columns):
    """
    Set the global DATATABLE, COLUMN_INDEXES, and COLUMN_TYPES.
    Converts any column marked as 'date' (in the columns dict) from a string (in 'YYYY-MM-DD' format)
    to a datetime.date object.
    
    Parameters:
        datatable (list): List of rows (each row is a list/tuple).
        col_indexes (dict or list): Either a dict mapping column names to indices or a list of column names.
        columns (dict): Mapping of column names to their types.
    """
    global DATATABLE, COLUMN_INDEXES, COLUMN_TYPES
    expected_columns = list(columns.keys())
    COLUMN_INDEXES = normalize_column_indexes(col_indexes, expected_columns)
    COLUMN_TYPES = {col: str(columns[col]).lower() for col in columns}
    
    new_datatable = []
    for row in datatable:
        new_row = list(row)
        for col, idx in COLUMN_INDEXES.items():
            if COLUMN_TYPES.get(col) == 'date' and isinstance(new_row[idx], str):
                new_row[idx] = datetime.datetime.strptime(new_row[idx], '%Y-%m-%d').date()
        new_datatable.append(new_row)
    DATATABLE = new_datatable

def set_datatable(datatable):
    """
    Reset the global DATATABLE (for example, after filtering).
    Reapply the date conversion using COLUMN_TYPES and COLUMN_INDEXES if available.
    """
    global DATATABLE
    if COLUMN_TYPES and COLUMN_INDEXES:
        new_datatable = []
        for row in datatable:
            new_row = list(row)
            for col, idx in COLUMN_INDEXES.items():
                if COLUMN_TYPES.get(col) == 'date' and isinstance(new_row[idx], str):
                    new_row[idx] = datetime.datetime.strptime(new_row[idx], '%Y-%m-%d').date()
            new_datatable.append(new_row)
        DATATABLE = new_datatable
    else:
        DATATABLE = datatable

###############################################################################
# Condition Evaluation
###############################################################################

def evaluate_condition(condition, row):
    """
    Recursively evaluate a condition structure against a row.
    
    The condition can be:
      - A leaf condition with keys 'column', 'operator', and 'value'
      - A compound condition with an 'operator' key ('AND', 'OR', or 'NOT') and either a list of subconditions
        (key "conditions") or a single subcondition (key "condition").
    
    Returns:
        bool: True if the condition is met by the row, False otherwise.
    
    Raises:
        KeyError: If a referenced column is not found in COLUMN_INDEXES.
        Exception: If an unknown operator is encountered.
    """
    if isinstance(condition, dict) and "column" in condition:
        # Leaf condition.
        col = condition.get("column")
        operator = condition.get("operator")
        expected_value = condition.get("value")
        index = COLUMN_INDEXES.get(col)
        if index is None:
            raise KeyError(f"Column '{col}' not found in COLUMN_INDEXES.")
        actual_value = row[index]
        if isinstance(actual_value, datetime.date) and isinstance(expected_value, str):
            expected_value = datetime.datetime.strptime(expected_value, '%Y-%m-%d').date()
        elif isinstance(expected_value, datetime.date) and isinstance(actual_value, str):
            actual_value = datetime.datetime.strptime(actual_value, '%Y-%m-%d').date()
        if operator in ['=', '==']:
            return actual_value == expected_value
        elif operator == '>':
            return actual_value > expected_value
        elif operator == '<':
            return actual_value < expected_value
        elif operator == '>=':
            return actual_value >= expected_value
        elif operator == '<=':
            return actual_value <= expected_value
        elif operator == '!=':
            return actual_value != expected_value
        else:
            raise Exception(f"Unknown operator in leaf condition: {operator}")
    elif isinstance(condition, dict):
        # Compound condition.
        op = condition.get("operator", "").upper()
        if op == "AND":
            return all(evaluate_condition(sub, row) for sub in condition.get("conditions", []))
        elif op == "OR":
            return any(evaluate_condition(sub, row) for sub in condition.get("conditions", []))
        elif op == "NOT":
            return not evaluate_condition(condition.get("condition"), row)
        else:
            raise Exception(f"Unknown compound operator: {op}")
    else:
        return bool(condition)


'''