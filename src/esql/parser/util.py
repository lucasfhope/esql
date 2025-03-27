import re
import numpy as np
import pandas as pd
from datetime import datetime

from src.esql.parser.constants import CONDITIONAL_OPERATORS
from src.esql.parser.error import ParsingError, ParsingErrorType
from src.esql.parser.types import ParsedSelectClause, GlobalAggregate, GroupAggregate, AggregatesDict, ParsedWhereClause, SimpleCondition, CompoundCondition, NotCondition, LogicalOperator, ParsedSuchThatClause, SimpleGroupCondition, CompoundGroupCondition, NotGroupCondition, ParsedHavingClause, CompoundHavingCondition, NotHavingCondition, GlobalHavingCondition, GroupHavingCondition


###########################################################################
# Keyword & Clause Extraction
###########################################################################
def get_keyword_clauses(query: str) -> dict[str, str]:
    '''
    Split the query into clauses based on the keywords.

    Parameters:
        query: The full query string (keywords should be lowercase).

    Returns:
        List of clause strings corresponding to each keyword in the keywords list.

    Raises:
        ParsingError: If the query is missing SELECT or if keywords appear out of order.
    '''
    keyword_clauses = {
        "SELECT": "",
        "OVER": "",
        "WHERE": "",
        "SUCH THAT": "",
        "HAVING": "",
        "ORDER BY": ""
    }

    # Find the location of each keyword in the query.
    keyword_indices = []
    for keyword in (kw.lower() for kw in keyword_clauses.keys()):
        pattern = r'\b' + re.escape(keyword.strip()) + r'\b'
        matches = list(re.finditer(pattern, query))
        if matches:
            keyword_indices.append(matches[0].start())
        else:
            keyword_indices.append(-1)

    # Check that 'select' is the first keyword and starts the query.
    if keyword_indices[0] != 0:
        raise ParsingError(ParsingErrorType.SELECT_CLAUSE, "Every query must start with SELECT")

    # Extract clauses based on keyword positions.
    previous_index = 0
    keywords = list(keyword_clauses.keys())
    previous_keyword = keywords[0]
    for keyword, keyword_index in zip(keywords[1:], keyword_indices[1:]):
        if keyword_index == -1:
            continue
        if keyword_index < previous_index:
            raise ParsingError(ParsingErrorType.CLAUSE_ORDER, f"Unexpected position of '{keyword.strip().upper()}'")
        clause = query[previous_index + len(previous_keyword):keyword_index].strip()
        if not clause:
            raise ParsingError(ParsingErrorType.MISSING_CLAUSE, f"No {previous_keyword.strip().upper()} argument found")
        keyword_clauses[previous_keyword] = clause
        previous_index = keyword_index
        previous_keyword = keyword

    clause = query[previous_index + len(previous_keyword):].strip()
    if not clause:
        raise ParsingError(ParsingErrorType.MISSING_CLAUSE, f"No {previous_keyword.strip().upper()} argument found")
    keyword_clauses[previous_keyword] = clause
            
    return keyword_clauses


###########################################################################
# SELECT Clause Parsing
###########################################################################
def parse_select_clause(select_clause: str, groups: list[str], columns: dict[str, np.dtype]) -> ParsedSelectClause:
    '''
    Parse the SELECT clause into columns and aggregate expressions.

    Parameters:
        select_clause: The SELECT clause string.
        groups: List of valid group identifiers.
        columns: Dictionary of available columns and their types.

    Returns:
        ParsedSelectClause: A dictionary containing the parsed columns and aggregates.

    Raises:
        ParsingError: If an invalid column or aggregate expression is encountered.
    '''
    columns_list: str = []
    aggregates: AggregatesDict = {
        "global_scope": [],
        "group_specific": []
    }
    
    for item in (s.strip() for s in select_clause.split(',')):
        if '.' in item:
            aggregate_result = parse_aggregate(item, groups, columns, ParsingErrorType.SELECT_CLAUSE)
            if 'group' in aggregate_result:
                aggregates['group_specific'].append(aggregate_result)
            else:
                aggregates['global_scope'].append(aggregate_result)
        else:
            if item in columns:
                columns_list.append(item)
            else:
                raise ParsingError(ParsingErrorType.SELECT_CLAUSE, f"Invalid column: '{item}'")

    return {
        'columns': columns_list,
        'aggregates': aggregates
    }


###########################################################################
# WHERE Clause Parsing
###########################################################################
def parse_where_clause(where_clause: str, columns: dict[str, np.dtype]) -> ParsedWhereClause:
    '''
    Parse the WHERE clause into a nested structure with support for logical operators and parentheses.

    Parameters:
        where_clause: The WHERE clause string.
        columns: Dictionary of available columns.

    Returns:
        ParsedWhereClause: A compound dictionary representing the parsed WHERE clause.
    '''
    where_clause = where_clause.strip()

    # Remove outer parentheses if they wrap the entire clause.
    if where_clause.startswith('(') and where_clause.endswith(')'):
        paren_level = 0
        is_outermost = True
        for i, char in enumerate(where_clause):
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            if paren_level == 0 and i < len(where_clause) - 1:
                is_outermost = False
                break
        if is_outermost:
            return parse_where_clause(where_clause[1:-1].strip(), columns)

    # Split by top-level OR operators.
    or_conditions = split_by_operator(where_clause, 'or')
    if len(or_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.OR,
            conditions=[parse_where_clause(cond, columns) for cond in or_conditions]
        )

    # Split by top-level AND operators.
    and_conditions = split_by_operator(where_clause, 'and')
    if len(and_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.AND,
            conditions=[parse_where_clause(cond, columns) for cond in and_conditions]
        )

    # Handle a leading NOT operator.
    if where_clause.lower().startswith('not '):
        condition = where_clause[4:].strip()
        if condition.startswith('(') and condition.endswith(')'):
            condition = condition[1:-1].strip()
        return NotCondition(
            operator=LogicalOperator.NOT,
            condition=parse_where_clause(condition, columns)
        )      
    
    # Parse as a SimpleCondition.
    return parse_where_condition(where_clause, columns)


def parse_where_condition(condition: str, columns: dict[str, np.dtype]) -> SimpleCondition:
    '''
    Parse a SimpleCondition for the WHERE clause.

    Parameters:
        condition: The condition string.
        columns: Dictionary of available columns and their data types.

    Returns:
        dict: A dictionary representing the condition.

    Raises:
        ParsingError: If the condition is invalid or uses an unknown column.
    '''
    condition = condition.strip()
    if condition.startswith('(') and condition.endswith(')'):
        condition = condition[1:-1].strip()

    operator_pattern = r'\s*(' + '|'.join(re.escape(op) for op in CONDITIONAL_OPERATORS) + r')\s*'
    match = re.search(operator_pattern, condition)
    if not match:
        if condition in columns and columns[condition] == np.bool_:
            return SimpleCondition(
                column=condition,
                operator='=',
                value=True
            )
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid condition: '{condition}'")

    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid condition: {condition}")

    column = parts[0].strip()
    value = parts[1].strip()
    if operator and value == '':
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Missing value for condition: {condition}")

    if operator in ['>=', '<=', '>', '<']:
        value = value.strip()
        if re.match(r"^'\d{4}-\d{2}-\d{2}'$|^\"\d{4}-\d{2}-\d{2}\"$", value):
            value = datetime.strptime(value.strip("'\""), '%Y-%m-%d').date()
        else:
            try:
                value = float(value)
            except ValueError:
                raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid value for condition: {condition}")
    elif operator in ['=', '==', '!=']:
        value = value.strip()
        if value.lower() in ['true', 'false']:
            value = value.lower() == 'true'
        elif re.match(r"^'\d{4}-\d{2}-\d{2}'$|^\"\d{4}-\d{2}-\d{2}\"$", value):
            value = datetime.strptime(value.strip("'\""), '%Y-%m-%d').date()
        elif (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        elif value.isdigit():
            value = float(value)
        elif value is None:
            raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Missing value for condition: {condition}")
        else:
            raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid value for condition: {condition}")
    else:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid operator: {operator}")

    if column not in columns:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid column: {column}")

    return SimpleCondition(
        column=column,
        operator=operator,
        value=value
    )


###########################################################################
# SUCH THAT Clause Parsing
###########################################################################
def parse_such_that_clause(condition: str, groups: list[str], columns: dict[str, np.dtype]) -> ParsedSuchThatClause:
    '''
    Parse the condition statement for a specific group ensuring that each section contains only one group.
    
    This function supports logical operators (OR, AND, NOT) and nested parentheses with the following precedence:
      1. OR (lowest)
      2. AND
      3. NOT (highest)

    Parameters:
        condition: The SUCH THAT clause condition.
        groups: List of valid group identifiers.
        columns: Dictionary of available columns.

    Returns:
        ParsedSuchThatClause: A nested dictionary representing the parsed SUCH THAT clause.
    
    Raises:
        ParsingError: If the condition is invalid or references multiple groups.
    '''
    condition = condition.strip()

    if condition.startswith('(') and condition.endswith(')'):
        paren_level = 0
        is_outermost = True
        for i, char in enumerate(condition):
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            if paren_level == 0 and i < len(condition) - 1:
                is_outermost = False
                break
        if is_outermost:
            return parse_such_that_clause(condition[1:-1].strip(), groups, columns)

    or_conditions = split_by_operator(condition, 'or')
    if len(or_conditions) > 1:
        parsed_or_conditions = [parse_such_that_clause(cond, groups, columns) for cond in or_conditions]
        groups_found = {groupCondition['group'] for groupCondition in parsed_or_conditions if 'group' in groupCondition}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, "Multiple groups found in a section. Each section must contain only one group.")
        return CompoundGroupCondition(
            group=parsed_or_conditions[0]['group'],
            operator=LogicalOperator.OR,
            conditions=parsed_or_conditions
        )

    and_conditions = split_by_operator(condition, 'and')
    if len(and_conditions) > 1:
        parsed_and_conditions = [parse_such_that_clause(cond, groups, columns) for cond in and_conditions]
        groups_found = {sub['group'] for sub in parsed_and_conditions if 'group' in sub}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, "Multiple groups found in a section. Each section must contain only one group.")
        return CompoundGroupCondition(
            group=parsed_or_conditions[0]['group'],
            operator=LogicalOperator.AND,
            conditions=parsed_or_conditions
        )

    if condition.lower().startswith('not '):
        inner_condition = condition[4:].strip()
        if inner_condition.startswith('(') and inner_condition.endswith(')'):
            inner_condition = inner_condition[1:-1].strip()
        return NotGroupCondition(
            operator=LogicalOperator.NOT,
            condition=parse_such_that_clause(inner_condition, groups, columns)
        )

    # Validate that the condition begins with a valid group.
    group_found = None
    for group in groups:
        if condition.startswith(group + '.'):
            group_found = group
            break
    if not group_found:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid group condition: '{condition}'. No valid group found.")

    if any(other_group + '.' in condition for other_group in groups if other_group != group_found):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Condition '{condition}' contains multiple groups. Each section must contain only one group.")

    return parse_group_condition(condition, group_found, columns)


def parse_group_condition(condition: str, group: str, columns: dict[str, np.dtype]) -> SimpleGroupCondition:
    '''
    Parse a single condition for a specific group.

    Parameters:
        condition: The condition string.
        group: The expected group identifier.
        columns: Dictionary of available columns.

    Returns:
        SimpleGroupCondition: A dictionary representing the parsed group condition.

    Raises:
        ParsingError: If the condition is invalid.
    '''
    condition = condition.strip()
    if condition.startswith('(') and condition.endswith(')'):
        condition = condition[1:-1].strip()

    operator_pattern = r'\s*(' + '|'.join(re.escape(op) for op in CONDITIONAL_OPERATORS) + r')\s*'
    match = re.search(operator_pattern, condition)
    if not match:
        if condition.startswith(group + '.'):
            column_name = condition[len(group) + 1:].strip()
            if column_name in columns and columns[column_name] == np.bool_:
                return SimpleGroupCondition(
                    group=group,
                    column=column_name,
                    operator='=',
                    value=True
                )
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid condition: '{condition}'")
    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid condition: '{condition}'")

    left = parts[0].strip()
    right = parts[1].strip()

    if not left.startswith(group + '.'):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, "Invalid group for condition: '{condition}'")
    column = left[len(group) + 1:]
    if column not in columns:
        raise ParsingError(f"Invalid column: '{column}'")

    if operator in ['>=', '<=', '>', '<']:
        if re.match(r"^'\d{4}-\d{2}-\d{2}'$", right) or re.match(r'^"\d{4}-\d{2}-\d{2}"$', right):
            value = datetime.strptime(right.strip("'\""), '%Y-%m-%d').date()
        else:
            try:
                value = float(right)
            except ValueError:
                raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid value for condition: '{condition}'")
    elif operator in ['=', '==', '!=']:
        if right.lower() in ['true', 'false']:
            value = right.lower() == 'true'
        elif re.match(r"^'\d{4}-\d{2}-\d{2}'$", right) or re.match(r'^"\d{4}-\d{2}-\d{2}"$', right):
            value = datetime.strptime(right.strip("'\""), '%Y-%m-%d').date()
        elif right.isdigit():
            value = float(right)
        else:
            value = right.strip('"').strip("'")
    else:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid operator: '{operator}'")

    return SimpleGroupCondition(
        group=group,
        column=column,
        operator=operator,
        value=value
    )
       

###########################################################################
# HAVING Clause Parsing
###########################################################################
def parse_having_clause(condition: str, groups: list[str], columns: dict[str, np.dtype]) -> ParsedHavingClause:
    '''
    Parse the HAVING clause into a nested structure supporting AND, OR, and NOT operators.

    Parameters:

        condition: The HAVING clause string.
        groups: List of valid group identifiers.
        columns: Dictionary of available columns.

    Returns:
        ParsedHavingClause: A nested dictionary representing the parsed HAVING clause.

    Raises:
        ParsingError: If the condition is invalid.
    '''
    condition = condition.strip()
    if condition.startswith('(') and condition.endswith(')'):
        paren_level = 0
        is_outermost = True
        for i, char in enumerate(condition):
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            if paren_level == 0 and i < len(condition) - 1:
                is_outermost = False
                break
        if is_outermost:
            return parse_having_clause(condition[1:-1].strip(), groups, columns)

    or_conditions = split_by_operator(condition, 'or')
    if len(or_conditions) > 1:
        return {
            'operator': 'OR',
            'conditions': [parse_having_clause(cond, groups, columns) for cond in or_conditions]
        }

    and_conditions = split_by_operator(condition, 'and')
    if len(and_conditions) > 1:
        return {
            'operator': 'AND',
            'conditions': [parse_having_clause(cond, groups, columns) for cond in and_conditions]
        }

    if condition.lower().startswith('not '):
        inner_condition = condition[4:].strip()
        if inner_condition.startswith('(') and inner_condition.endswith(')'):
            inner_condition = inner_condition[1:-1].strip()
        return {
            'operator': 'NOT',
            'condition': parse_having_clause(inner_condition, groups, columns)
        }

    return parse_having_condition(condition, groups, columns)


def parse_having_condition(condition: str, groups: list[str], columns: dict[str, np.dtype]) ->  GroupHavingCondition | GlobalHavingCondition:
    '''
    Parse a single HAVING condition with dot notation (e.g., quant.sum > 100 or g1.quant.avg = 50).

    Parameters:
        condition: The condition string.
        groups: List of valid group identifiers.
        columns: Dictionary of available columns.

    Returns:
        GroupHavingCondition | GlobalHavingCondition: A dictionary representing the parsed HAVING condition.
    
    Raises:
        ParsingError: If the aggregate expression or comparison is invalid.
    '''
    condition = condition.strip()
    operator_pattern = r'\s*(' + '|'.join(re.escape(op) for op in CONDITIONAL_OPERATORS) + r')\s*'
    match = re.search(operator_pattern, condition)
    if not match:
        raise ParsingError(f"Invalid HAVING condition: {condition}")
    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(f"Invalid HAVING condition: {condition}")
    
    left = parts[0].strip()  # e.g., quant.sum or g1.quant.avg
    right = parts[1].strip()  # e.g., 100

    aggregate_parts = left.split('.')
    if len(aggregate_parts) == 2:  # Global aggregate
        column, func = aggregate_parts
        group = None
    elif len(aggregate_parts) == 3:  # Group-specific aggregate
        group, column, func = aggregate_parts
    else:
        raise ParsingError(f"Invalid aggregate expression: {left}")

    if group is not None and group not in groups:
        raise ParsingError(f"Invalid group in aggregate: {group}")
    if column not in columns:
        raise ParsingError(f"Invalid column in aggregate: {column}")
    if func not in AGGREGATE_FUNCTIONS:
        raise ParsingError(f"Invalid aggregate function: {func}")

    try:
        parsed_value = float(right)
    except ValueError:
        raise ParsingError(f"Invalid value for condition: {right}")

    if group is not None:
        return GroupHavingCondition(
            group=group,
            column=column,
            function=func,
            oprerator=operator,
            value=parsed_value
        )
    return GlobalHavingCondition(
        column=column,
        function=func,
        operator=operator,
        value=parsed_value
    )


###########################################################################
# Helper Functions
###########################################################################
def parse_aggregate(aggregate: str, groups: list[str], columns: dict[str, np.dtype], error_type=ParsingErrorType.SELECT_CLAUSE or ParsingErrorType.HAVING_CLAUSE) -> GlobalAggregate | GroupAggregate:
    '''
    Parse an aggregate expression using dot notation (e.g., column.agg or group.column.agg).

    Parameters:
        aggregate: The aggregate expression.
        groups: List of valid group identifiers.
        columns: Dictionary of available columns.

    Returns:
        GlobalAggregate | GroupAggregate: The parsed aggregate.
    
    Raises:
        ParsingError the aggregate is invalid.
    '''
    AGGREGATE_FUNCTIONS = ['sum','avg','min', 'max', 'count']
    parts = aggregate.split('.')

    # Format: column.aggregate_function
    if len(parts) == 2:
        column, func = parts
        if column not in columns:
            raise ParsingError(error_type, f"Invalid aggregate column: '{aggregate}'")
        elif func not in AGGREGATE_FUNCTIONS:
            raise ParsingError(error_type, f"Invalid aggregate function: '{aggregate}'")
        elif func != 'count' and not (pd.api.types.is_any_real_numeric_dtype(columns[column])):
            raise ParsingError(error_type, f"Invalid aggregate. Column is not a numeric type: '{aggregate}'")
        return GlobalAggregate(
            column=column,
            function=func,
            datatype=float
        )
    
    # Format: group.column.aggregate_function
    elif len(parts) == 3:
        group, column, func = parts
        if group not in groups:
            raise ParsingError(error_type, f"Invalid aggregate group: '{aggregate}'")
        elif column not in columns:
            raise ParsingError(error_type, f"Invalid aggregate column: '{aggregate}'")
        elif func not in AGGREGATE_FUNCTIONS:
            raise ParsingError(error_type, f"Invalid aggregate function: '{aggregate}'")
        elif func != 'count' and not (pd.api.types.is_any_real_numeric_dtype(columns[column])):
            raise ParsingError(error_type, f"Invalid aggregate. Column is not a numeric type: '{aggregate}'")
        return GroupAggregate(
            group=group,
            column=column,
            function=func,
            datatype=float
        )
    
    raise ParsingError(error_type, f"Invalid aggregate: '{aggregate}'\nAggregate must be in the format 'column.aggregate_function' or 'group.column.aggregate_function'")


def collect_having_aggregates(having_condition: ParsedHavingClause) -> AggregatesDict:
    '''
    Recursively collect aggregate conditions from a ParsedHavingClause.

    Parameters:
        having_condition: A ParsedHavingClause dict.

    Returns:
        AggregatesDict: A dictionary of global and group-specific aggregates.
    '''
    aggregates: AggregatesDict = {
        "global_scope": [],
        "group_specific": []
    }

    if all(k in having_condition for k in ['column', 'function', 'operator', 'value']):
        if 'group' in having_condition:
            aggregates['group_specific'].append(
                GroupAggregate(
                    group=having_condition['group'],
                    column=having_condition['column'],
                    function=having_condition['function'],
                    datatype=float
                )
            )
        else:
            aggregates['global_scope'].append(
                GlobalAggregate(
                    column=having_condition['column'],
                    function=having_condition['function'],
                    datatype=float
                )
            )
    # Compound condition with multiple subconditions.
    elif 'conditions' in having_condition:
        for condition in having_condition['conditions']:
            condition_aggregates = collect_having_aggregates(condition)
            aggregates['global_scope'].extend(condition_aggregates['global_scope'])
            aggregates['group_specific'].extend(condition_aggregates['group_specific'])
    # NOT condition, which has a single nested condition.
    elif 'condition' in having_condition:
        sub_aggregates = collect_having_aggregates(having_condition['condition'])
        aggregates['global_scope'].extend(sub_aggregates['global_scope'])
        aggregates['group_specific'].extend(sub_aggregates['group_specific'])

    return aggregates


def split_by_operator(condition: str, operator: str) -> list[str]:
    '''
    Split a condition by the given operator while respecting nested parentheses.

    Parameters:
        condition (str): The condition string.
        operator (str): The logical operator (e.g., 'and', 'or') to split by.

    Returns:
        list: A list of condition parts.
    '''
    parts = []
    current = ''
    paren_level = 0
    i = 0

    while i < len(condition):
        char = condition[i]

        # Track parentheses.
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1

        # Check for the operator only when not inside parentheses.
        if (char == ' ' and 
            i + 1 + len(operator) <= len(condition) and 
            condition[i + 1:i + 1 + len(operator)].lower() == operator and 
            paren_level == 0):
            parts.append(current.strip())
            current = ''
            i += len(operator) + 1  # Skip operator and following space.
        else:
            current += char
            i += 1

    if current.strip():
        parts.append(current.strip())

    return parts