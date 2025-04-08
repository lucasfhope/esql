import re
import numpy as np
import pandas as pd
from datetime import datetime, date

from src.esql.parser.error import ParsingError, ParsingErrorType
from src.esql.parser.types import ParsedSelectClause, GlobalAggregate, GroupAggregate, AggregatesDict, ParsedWhereClause, SimpleCondition, CompoundCondition, NotCondition, LogicalOperator, ParsedSuchThatClause, SimpleGroupCondition, CompoundGroupCondition, NotGroupCondition, ParsedHavingClause, CompoundAggregateCondition, NotAggregateCondition, GlobalAggregateCondition, GroupAggregateCondition


###########################################################################
# Keyword & Clause Extraction
###########################################################################
def get_keyword_clauses(query: str) -> dict[str, str]:
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
# OVER Clause Parsing
###########################################################################
def parse_over_clause(over_clause: str) -> list[str]:
    groups = []
    pattern = r"^[a-zA-Z0-9_]+$"
    for group in (g.strip() for g in over_clause.split(",")):
        match = re.match(pattern, group)
        if not match:
            raise ParsingError(ParsingErrorType.OVER_CLAUSE, f"Invalid group name: '{group}")
        groups.append(group)
    return groups


###########################################################################
# SELECT Clause Parsing
###########################################################################
def parse_select_clause(select_clause: str, groups: list[str], column_dtypes: dict[str, np.dtype]) -> ParsedSelectClause:
    columns: str = []
    aggregates = AggregatesDict(
        global_scope=[],
        group_specific=[]
    )
    
    for item in (s.strip() for s in select_clause.split(',')):
        if '.' in item:
            aggregate_result = _parse_aggregate(
                aggregate=item, 
                groups=groups,
                column_dtypes=column_dtypes,
                error_type=ParsingErrorType.SELECT_CLAUSE
            )
            if 'group' in aggregate_result:
                aggregates['group_specific'].append(aggregate_result)
            else:
                aggregates['global_scope'].append(aggregate_result)
        else:
            if item in column_dtypes:
                columns.append(item)
            else:
                raise ParsingError(ParsingErrorType.SELECT_CLAUSE, f"Invalid column: '{item}'")

    return ParsedSelectClause(
        columns=columns,
        aggregates=aggregates
    )


###########################################################################
# WHERE Clause Parsing
###########################################################################
def parse_where_clause(where_clause: str, column_dtypes: dict[str, np.dtype]) -> ParsedWhereClause:
    where_clause = where_clause.strip()
    if _has_wrapping_parenthesis(where_clause):
        return parse_where_clause(where_clause[1:-1].strip(), column_dtypes)

    or_conditions = _split_by_logical_operator(where_clause, LogicalOperator.OR)
    if len(or_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.OR,
            conditions=[parse_where_clause(cond, column_dtypes) for cond in or_conditions]
        )

    and_conditions = _split_by_logical_operator(where_clause, LogicalOperator.AND)
    if len(and_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.AND,
            conditions=[parse_where_clause(cond, column_dtypes) for cond in and_conditions]
        )

    if where_clause.lower().startswith(LogicalOperator.NOT.value.lower()+' '):
        condition = where_clause[len(LogicalOperator.NOT.value)+1:].strip()
        return NotCondition(
            operator=LogicalOperator.NOT,
            condition=parse_where_clause(condition, column_dtypes)
        )     
    
    return _parse_simple_condition(where_clause, column_dtypes)


def _parse_simple_condition(condition: str, column_dtypes: dict[str, np.dtype]) -> SimpleCondition:
    condition = condition.strip()
    match = _find_conditional_operator(condition)
    if not match:
        if condition in column_dtypes and pd.api.types.is_bool_dtype(column_dtypes[condition]):
            return SimpleCondition(
                column=condition,
                operator='=',
                value=True,
                is_emf = False
            )
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"No conditional operator found in condition: '{condition}'")

    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid condition: {condition}")

    column = parts[0].strip()
    value = parts[1].strip()

    if column not in column_dtypes:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid column: {column}")
    if operator and value == '':
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Missing value for condition: {condition}")
    
    value, is_emf = _parse_condition_value(column_dtypes[column], operator, value, column_dtypes, condition, ParsingErrorType.WHERE_CLAUSE)
    return SimpleCondition(
        column=column,
        operator=operator,
        value=value,
        is_emf=is_emf
    )


###########################################################################
# SUCH THAT Clause Parsing
###########################################################################
def parse_such_that_clause(such_that_clause: str, groups: list[str], column_dtypes: dict[str, np.dtype]) -> ParsedSuchThatClause:
    such_that_clause = such_that_clause.strip()
    if _has_wrapping_parenthesis(such_that_clause):
        return parse_such_that_clause(such_that_clause[1:-1].strip(), groups, column_dtypes)

    or_conditions = _split_by_logical_operator(such_that_clause, LogicalOperator.OR)
    if len(or_conditions) > 1:
        parsed_or_conditions = [parse_such_that_clause(cond, groups, column_dtypes) for cond in or_conditions]
        groups_found = {groupCondition['group'] for groupCondition in parsed_or_conditions if 'group' in groupCondition}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{such_that_clause}'\nEach comma seperated clause must contain only one group.")
        return CompoundGroupCondition(
            operator=LogicalOperator.OR,
            conditions=parsed_or_conditions
        )

    and_conditions = _split_by_logical_operator(such_that_clause, LogicalOperator.AND)
    if len(and_conditions) > 1:
        parsed_and_conditions = [parse_such_that_clause(cond, groups, column_dtypes) for cond in and_conditions]
        groups_found = {groupCondition['group'] for groupCondition in parsed_and_conditions if 'group' in groupCondition}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{such_that_clause}'\nEach comma seperated clause must contain only one group.")
        return CompoundGroupCondition(
            operator=LogicalOperator.AND,
            conditions=parsed_and_conditions
        )

    if such_that_clause.lower().startswith(LogicalOperator.NOT.value.lower()+' '):
        condition = such_that_clause[len(LogicalOperator.NOT.value)+1:].strip()
        return NotGroupCondition(
            operator=LogicalOperator.NOT,
            condition=parse_such_that_clause(condition, groups, column_dtypes)
        )

    group_found = None
    for group in groups:
        if such_that_clause.startswith(group + '.'):
            group_found = group
            break
    if not group_found:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"No valid group found in condition: '{such_that_clause}'")

    if any(other_group + '.' in such_that_clause for other_group in groups if other_group != group_found):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{such_that_clause}'\nEach comma seperated clause must contain only one group.")
    
    return _parse_simple_group_condition(such_that_clause, group_found, column_dtypes)
    

def _parse_simple_group_condition(condition: str, group: str, column_dtypes: dict[str, np.dtype]) -> SimpleGroupCondition:
    condition = condition.strip()
    match = _find_conditional_operator(condition)
    if not match:
        if condition.startswith(group + '.'):
            column = condition[len(group) + 1:].strip()
            if column in column_dtypes and pd.api.types.is_bool_dtype(column_dtypes[column]):
                return SimpleGroupCondition(
                    group=group,
                    column=column,
                    operator='=',
                    value=True,
                    is_emf=False
                )
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid condition: '{condition}'")
    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid condition: '{condition}'")

    left = parts[0].strip()
    value = parts[1].strip()

    if not left.startswith(group + '.'):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid group for condition: '{condition}'")
    column = left[len(group) + 1:]
    if column not in column_dtypes:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid column: '{column}'")
    if operator and value == '':
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Missing value for condition: {condition}")

    value, is_emf = _parse_condition_value(column_dtypes[column], operator, value, column_dtypes, condition, ParsingErrorType.SUCH_THAT_CLAUSE)
    return SimpleGroupCondition(
        group=group,
        column=column,
        operator=operator,
        value=value,
        is_emf=is_emf
    )
       

###########################################################################
# HAVING Clause Parsing
###########################################################################
def parse_having_clause(having_clause: str, groups: list[str], column_dtypes: dict[str, np.dtype]) -> tuple[ParsedHavingClause, AggregatesDict]:
    return _parse_having_clause(
        having_clause=having_clause,
        aggregates=AggregatesDict(
            global_scope=[],
            group_specific=[]
        ),
        groups=groups,
        column_dtypes=column_dtypes
    )


def _parse_having_clause(having_clause: str, aggregates: AggregatesDict, groups: list[str], column_dtypes: dict[str, np.dtype]) -> tuple[ParsedHavingClause, AggregatesDict]:
    having_clause = having_clause.strip()
    if _has_wrapping_parenthesis(having_clause):
        return _parse_having_clause(having_clause[1:-1].strip(), aggregates, groups, column_dtypes)

    or_conditions = _split_by_logical_operator(having_clause, LogicalOperator.OR)
    if len(or_conditions) > 1:
        conditions = []
        for condition in or_conditions:
            cond, aggregates = _parse_having_clause(condition, aggregates, groups, column_dtypes)
            conditions.append(cond)
        return (
            CompoundAggregateCondition(
                operator=LogicalOperator.OR,
                conditions=conditions
            ),
            aggregates
        )
            
    and_conditions = _split_by_logical_operator(having_clause, LogicalOperator.AND)
    if len(and_conditions) > 1:
        conditions = []
        for condition in and_conditions:
            cond, aggregates = _parse_having_clause(condition, aggregates, groups, column_dtypes)
            conditions.append(cond)
        return (
            CompoundAggregateCondition(
                operator=LogicalOperator.AND,
                conditions=conditions
            ),
            aggregates
        )

    if having_clause.lower().startswith(LogicalOperator.NOT.value.lower()+' '):
        having_clause = having_clause[len(LogicalOperator.NOT.value)+1:].strip()
        condition, aggregates = _parse_having_clause(having_clause, aggregates, groups, column_dtypes)
        return (
            NotGroupCondition(
                operator=LogicalOperator.NOT,
                condition=condition
            ),
            aggregates
        )

    return _parse_aggregate_condition(having_clause, aggregates, groups, column_dtypes)


def _parse_aggregate_condition(condition: str, aggregates: AggregatesDict, groups: list[str], column_dtypes: dict[str, np.dtype]) ->  tuple[GroupAggregateCondition | GlobalAggregateCondition, AggregatesDict]:
    condition = condition.strip()
    match = _find_conditional_operator(condition)
    if not match:
        raise ParsingError(ParsingErrorType.HAVING_CLAUSE, f"Invalid condition: '{condition}'")
    operator = match.group(1)
    parts = re.split(r'\s*' + re.escape(operator) + r'\s*', condition)
    if len(parts) != 2:
        raise ParsingError(ParsingErrorType.HAVING_CLAUSE, f"Invalid condition: '{condition}'")
    
    aggregate_str = parts[0].strip()
    value = parts[1].strip()

    aggregate: Global_Aggregate | GroupAggregate = _parse_aggregate(aggregate_str, groups, column_dtypes, ParsingErrorType.HAVING_CLAUSE)
    
    try:
        value = float(value)
    except ValueError:
        raise ParsingError(ParsingErrorType.HAVING_CLAUSE, f"Invalid value for condition: {condition}")

    if 'group' in aggregate:
        if aggregate not in aggregates['group_specific']:
            aggregates['group_specific'].append(aggregate)
        return (
            GroupAggregateCondition(
                aggregate=aggregate,
                operator=operator,
                value=value
            ),
            aggregates
        )
    if aggregate not in aggregates['global_scope']:
        aggregates['global_scope'].append(aggregate)
    return (
        GlobalAggregateCondition(
            aggregate=aggregate,
            operator=operator,
            value=value
        ),
        aggregates
    )


###########################################################################
# ORDER BY Clause Parsing
###########################################################################
def parse_order_by_clause(order_by_clause: str, number_of_select_columns: int):
    try:
        order_value = int(order_by_clause.strip())  
    except ValueError:
        raise ParsingError(ParsingErrorType.ORDER_BY_CLAUSE, f"Invalid value: '{order_by_clause}'")
    if order_value < 1 or order_value > number_of_select_columns:
        raise ParsingError(ParsingErrorType.ORDER_BY_CLAUSE, f"Value out of range of selected columns: '{order_by_clause}'")
    return order_value


###########################################################################
# Aggregate and Value Parsing
###########################################################################
def _parse_aggregate(aggregate: str, groups: list[str], column_dtypes: dict[str, np.dtype], error_type=ParsingErrorType.SELECT_CLAUSE or ParsingErrorType.HAVING_CLAUSE) -> GlobalAggregate | GroupAggregate:
    AGGREGATE_FUNCTIONS = ['sum','avg','min', 'max', 'count']
    parts = aggregate.split('.')

    # Format: column.aggregate_function
    if len(parts) == 2:
        column, func = parts
        if column not in column_dtypes:
            raise ParsingError(error_type, f"Invalid aggregate column: '{aggregate}'")
        elif func not in AGGREGATE_FUNCTIONS:
            raise ParsingError(error_type, f"Invalid aggregate function: '{aggregate}'")
        elif func != 'count' and not (pd.api.types.is_any_real_numeric_dtype(column_dtypes[column])):
            raise ParsingError(error_type, f"Invalid aggregate. Column is not a numeric type: '{aggregate}'")
        return GlobalAggregate(
            column=column,
            function=func
        )
    
    # Format: group.column.aggregate_function
    elif len(parts) == 3:
        group, column, func = parts
        if group not in groups:
            raise ParsingError(error_type, f"Invalid aggregate group: '{aggregate}'")
        elif column not in column_dtypes:
            raise ParsingError(error_type, f"Invalid aggregate column: '{aggregate}'")
        elif func not in AGGREGATE_FUNCTIONS:
            raise ParsingError(error_type, f"Invalid aggregate function: '{aggregate}'")
        elif func != 'count' and not (pd.api.types.is_any_real_numeric_dtype(column_dtypes[column])):
            raise ParsingError(error_type, f"Invalid aggregate. Column is not a numeric type: '{aggregate}'")
        return GroupAggregate(
            group=group,
            column=column,
            function=func
        )
    
    raise ParsingError(error_type, f"Invalid aggregate: '{aggregate}'\nAggregate must be in the format 'column.aggregate_function' or 'group.column.aggregate_function'")


def _parse_condition_value(column_dtype: np.dtype, operator: str, value: str, column_dtypes: dict[str, np.dtype], condition: str, error_type=ParsingErrorType.SELECT_CLAUSE or ParsingErrorType.SUCH_THAT_CLAUSE) -> tuple[float | bool | str | date, bool]:
    date_pattern = r"^['\"]\d{4}[-/]\d{1,2}[-/]\d{1,2}['\"]$"
    value = value.strip()
    
    #TODO implement EMF parsing here
    if operator in ['>=', '<=', '>', '<']:
        if re.match(date_pattern, value) and pd.api.types.is_datetime64_any_dtype(column_dtype):
            try:
                return datetime.strptime(value[1:-1].replace('/', '-'), '%Y-%m-%d').date(), False
            except ValueError:
                raise ParsingError(error_type, f"Invalid date in condition: '{condition}'")
        elif pd.api.types.is_numeric_dtype(column_dtype):
            try:
                value = float(value)
                return int(value) if value.is_integer() else value, False
            except Exception:
                raise ParsingError(error_type, f"Invalid value in condition: '{condition}'")
        raise ParsingError(error_type, f"Invalid column reference or value in condition: '{condition}'")
            
    elif operator in ['=', '==', '!=']:
        if value.lower() in ['true', 'false'] and pd.api.types.is_bool_dtype(column_dtype):
            return value.lower() == 'true', False
        elif re.match(date_pattern, value) and pd.api.types.is_datetime64_any_dtype(column_dtype):
            try:
                return datetime.strptime(value[1:-1].replace('/', '-'), '%Y-%m-%d').date(), False
            except ValueError:
                raise ParsingError(error_type, f"Invalid date in condition: '{condition}'")
        elif (value.startswith("'") and value.endswith("'") or value.startswith('"') and value.endswith('"')) \
            and pd.api.types.is_string_dtype(column_dtype):
            return value[1:-1], False
        elif pd.api.types.is_numeric_dtype(column_dtype):
            try:
                value = float(value)
                return int(value) if value.is_integer() else value, False
            except Exception:
                raise ParsingError(error_type, f"Invalid value in condition: '{condition}'")
        raise ParsingError(error_type, f"Invalid column reference or value in condition: '{condition}'")
        
    raise ParsingError(error_type, f"Invalid operator in condition: '{condition}'")


#TODO: Implement to handle parsing of EMF values
# Should be able to handle numeric euquations (i.e col = col + 1) 
def _parse_emf_condition_value(value: str): 
    pass


###########################################################################
# Clause Structure Helper Functions
###########################################################################
def _find_conditional_operator(condition: str) -> re.Match | None:
    CONDITIONAL_OPERATORS = ['>=', '<=', '!=', '==', '>', '<', '=']
    operator_pattern = r'\s*(' + '|'.join(re.escape(op) for op in CONDITIONAL_OPERATORS) + r')\s*'
    match = re.search(operator_pattern, condition)
    return match


def _has_wrapping_parenthesis(condition: str) -> bool:
    if not (condition.startswith('(') and condition.endswith(')')):
        return False
    paren_level = 0
    for i, char in enumerate(condition):
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1
        if paren_level == 0 and i < len(condition) - 1:
            return False
    return True


def _split_by_logical_operator(condition: str, operator: LogicalOperator) -> list[str]:
    parts = []
    current = ''
    paren_level = 0
    i = 0
    while i < len(condition):
        char = condition[i]
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1

        # Check for the operator only when not inside parentheses.
        if (char == ' ' 
            and i + 1 + len(operator.value) <= len(condition) 
            and condition[i + 1:i + 1 + len(operator.value)].lower() == operator.value.lower()
            and paren_level == 0):
            parts.append(current.strip())
            current = ''
            i += len(operator.value) + 1
        else:
            current += char
            i += 1

    if current.strip():
        parts.append(current.strip())

    return parts



   
        

        