import re
import numpy as np
import pandas as pd
from datetime import datetime, date

from src.esql.parser.error import ParsingError, ParsingErrorType
from src.esql.parser.types import ParsedSelectClause, GlobalAggregate, GroupAggregate, AggregatesDict, ParsedWhereClause, SimpleCondition, CompoundCondition, NotCondition, LogicalOperator, ParsedSuchThatClause, ParsedSuchThatSection, SimpleGroupCondition, CompoundGroupCondition, NotGroupCondition, ParsedHavingClause, CompoundAggregateCondition, NotAggregateCondition, GlobalAggregateCondition, GroupAggregateCondition


###########################################################################
# Keyword & Clause Extraction
###########################################################################
def get_keyword_clauses(query: str) -> dict[str, str | None]:
    keyword_clauses = {
        "SELECT": None,
        "OVER": None,
        "WHERE": None,
        "SUCH THAT": None,
        "HAVING": None,
        "ORDER BY": None
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
def parse_over_clause(over_clause: str | None) -> list[str]:
    groups = []
    if over_clause == None:
        return None
    pattern = r"^[a-zA-Z0-9_]+$"
    for group in (group.strip() for group in over_clause.split(",")):
        match = re.match(pattern, group)
        if not match:
            raise ParsingError(ParsingErrorType.OVER_CLAUSE, f"Invalid group name: '{group}")
        groups.append(group)
    return groups


###########################################################################
# SELECT Clause Parsing
###########################################################################
def parse_select_clause(select_clause: str, groups: list[str], column_dtypes: dict[str, np.dtype]) -> ParsedSelectClause:
    grouping_attributes: str = []
    aggregates = AggregatesDict(
        global_scope=[],
        group_specific=[]
    )
    aggregates_in_order: List[GlobalAggregate | GroupAggregate] = []
    
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
                aggregates_in_order.append(aggregate_result)
            else:
                aggregates['global_scope'].append(aggregate_result)
                aggregates_in_order.append(aggregate_result)
        else:
            if item in column_dtypes:
                grouping_attributes.append(item)
            else:
                raise ParsingError(ParsingErrorType.SELECT_CLAUSE, f"Invalid column: '{item}'")
    if len(grouping_attributes) == 0:
        raise ParsingError(ParsingErrorType.SELECT_CLAUSE, f"No grouping attributes given: '{select_clause}'")

    return ParsedSelectClause(
        grouping_attributes=grouping_attributes,
        aggregates=aggregates,
        aggregates_in_order = aggregates_in_order
    )


###########################################################################
# WHERE Clause Parsing
###########################################################################
def parse_where_clause(where_clause: str | None, column_dtypes: dict[str, np.dtype]) -> ParsedWhereClause | None:
    if where_clause == None:
        return None
    return _parse_where_clause(where_clause, column_dtypes)

def _parse_where_clause(where_clause: str, column_dtypes: dict[str, np.dtype]) -> ParsedWhereClause:
    where_clause = where_clause.strip()
    if _has_wrapping_parenthesis(where_clause):
        return _parse_where_clause(where_clause[1:-1].strip(), column_dtypes)

    or_conditions = _split_by_logical_operator(where_clause, LogicalOperator.OR)
    if len(or_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.OR,
            conditions=[_parse_where_clause(cond, column_dtypes) for cond in or_conditions]
        )

    and_conditions = _split_by_logical_operator(where_clause, LogicalOperator.AND)
    if len(and_conditions) > 1:
        return CompoundCondition(
            operator=LogicalOperator.AND,
            conditions=[_parse_where_clause(cond, column_dtypes) for cond in and_conditions]
        )

    if where_clause.lower().startswith(LogicalOperator.NOT.value.lower()+' '):
        condition = where_clause[len(LogicalOperator.NOT.value)+1:].strip()
        return NotCondition(
            operator=LogicalOperator.NOT,
            condition=_parse_where_clause(condition, column_dtypes)
        )     
    
    return _parse_simple_condition(where_clause, column_dtypes)


def _parse_simple_condition(condition: str, column_dtypes: dict[str, np.dtype]) -> SimpleCondition:
    condition = condition.strip()
    split = _split_condition(condition)
    if not split:
        if condition in column_dtypes and pd.api.types.is_bool_dtype(column_dtypes[condition]):
            return SimpleCondition(
                column=condition,
                operator='=',
                value=True,
                is_emf = False
            )
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"No conditional operator found in condition: '{condition}'")

    column, operator, value = split
    if column not in column_dtypes:
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Invalid column: {column}")
    if operator and value == '':
        raise ParsingError(ParsingErrorType.WHERE_CLAUSE, f"Missing value for condition: {condition}")
    
    parsed_value, is_emf = _parse_condition_value(
        column_dtype=column_dtypes[column],
        operator=operator,
        value=value,
        column_dtypes=column_dtypes,
        condition=condition,
        error_type=ParsingErrorType.WHERE_CLAUSE
    )

    return SimpleCondition(
        column=column,
        operator=operator,
        value=parsed_value,
        is_emf=is_emf
    )


###########################################################################
# SUCH THAT Clause Parsing
###########################################################################
def parse_such_that_clause(such_that_clause: str | None, groups: list[str], column_dtypes: dict[str, np.dtype]) -> ParsedSuchThatClause | None:
    if such_that_clause == None:
        return None
    parsed_such_that_clause = []
    such_that_sections = such_that_clause.split(',')
    for section in such_that_sections:
        parsed_such_that_clause.append(
            _parse_such_that_section(
                section=section,
                groups=groups,
                column_dtypes=column_dtypes
            )
        )
    groups_in_parsed_clause = set()
    for section in parsed_such_that_clause:
        group = find_group_in_such_that_section(section)
        if group in groups_in_parsed_clause:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple sections contain group '{group}'.")
        groups_in_parsed_clause.add(group)
    return parsed_such_that_clause

def find_group_in_such_that_section(group_condition: ParsedSuchThatSection):
    if not group_condition:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, "No group found in group condition.")
    group = group_condition.get('group')
    if not group:
        return find_group_in_such_that_section(group_condition.get('condition') or group_condition.get('conditions')[0])
    return group

def _parse_such_that_section(section: str, groups: list[str], column_dtypes: dict[str, np.dtype]) -> ParsedSuchThatSection:
    section = section.strip()
    if _has_wrapping_parenthesis(section):
        return _parse_such_that_section(section[1:-1].strip(), groups, column_dtypes)

    or_conditions = _split_by_logical_operator(section, LogicalOperator.OR)
    if len(or_conditions) > 1:
        parsed_or_conditions = [_parse_such_that_section(cond, groups, column_dtypes) for cond in or_conditions]
        groups_found = {groupCondition['group'] for groupCondition in parsed_or_conditions if 'group' in groupCondition}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{section}'\nEach comma seperated clause must contain only one group.")
        return CompoundGroupCondition(
            operator=LogicalOperator.OR,
            conditions=parsed_or_conditions
        )

    and_conditions = _split_by_logical_operator(section, LogicalOperator.AND)
    if len(and_conditions) > 1:
        parsed_and_conditions = [_parse_such_that_section(cond, groups, column_dtypes) for cond in and_conditions]
        groups_found = {groupCondition['group'] for groupCondition in parsed_and_conditions if 'group' in groupCondition}
        if len(groups_found) != 1:
            raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{section}'\nEach comma seperated clause must contain only one group.")
        return CompoundGroupCondition(
            operator=LogicalOperator.AND,
            conditions=parsed_and_conditions
        )

    if section.lower().startswith(LogicalOperator.NOT.value.lower()+' '):
        condition = section[len(LogicalOperator.NOT.value)+1:].strip()
        return NotGroupCondition(
            operator=LogicalOperator.NOT,
            condition=_parse_such_that_section(condition, groups, column_dtypes)
        )

    group_found = None
    for group in groups:
        if section.startswith(group + '.'):
            group_found = group
            break
    if not group_found:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"No valid group found in condition: '{section}'")

    if any(other_group + '.' in section for other_group in groups if other_group != group_found):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Multiple groups found in a clause: '{section}'\nEach comma seperated clause must contain only one group.")
    
    return _parse_simple_group_condition(section, group_found, column_dtypes)
    

def _parse_simple_group_condition(condition: str, group: str, column_dtypes: dict[str, np.dtype]) -> SimpleGroupCondition:
    condition = condition.strip()
    split = _split_condition(condition)
    if not split:
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
    
    left, operator, value = split
    if not left.startswith(group + '.'):
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid group for condition: '{condition}'")
    column = left[len(group) + 1:]
    if column not in column_dtypes:
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Invalid column: '{column}'")
    if operator and value == '':
        raise ParsingError(ParsingErrorType.SUCH_THAT_CLAUSE, f"Missing value for condition: {condition}")

    parsed_value, is_emf = _parse_condition_value(
        column_dtype=column_dtypes[column],
        operator=operator,
        value=value,
        column_dtypes=column_dtypes,
        condition=condition,
        error_type=ParsingErrorType.SUCH_THAT_CLAUSE
    )

    return SimpleGroupCondition(
        group=group,
        column=column,
        operator=operator,
        value=parsed_value,
        is_emf=is_emf
    )
       

###########################################################################
# HAVING Clause Parsing
###########################################################################
def parse_having_clause(having_clause: str | None, groups: list[str], column_dtypes: dict[str, np.dtype]) -> tuple[ParsedHavingClause | None, AggregatesDict]:
    aggregates = AggregatesDict(
        global_scope=[],
        group_specific=[]
    )
    if having_clause == None:
        return (None, aggregates)
    return _parse_having_clause(
        having_clause=having_clause,
        aggregates=aggregates,
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
    split = _split_condition(condition)
    if not split:
        raise ParsingError(ParsingErrorType.HAVING_CLAUSE, f"No conditional operator found in condition: '{condition}'")
    left, operator, right = split
    aggregate: Global_Aggregate | GroupAggregate = _parse_aggregate(
        aggregate=left,
        groups=groups,
        column_dtypes=column_dtypes,
        error_type=ParsingErrorType.HAVING_CLAUSE
    )
    
    try:
        numeric_value = float(right)
    except ValueError:
        raise ParsingError(ParsingErrorType.HAVING_CLAUSE, f"Invalid value for condition: {condition}")

    if 'group' in aggregate:
        if aggregate not in aggregates['group_specific']:
            aggregates['group_specific'].append(aggregate)
        return (
            GroupAggregateCondition(
                aggregate=aggregate,
                operator=operator,
                value=numeric_value
            ),
            aggregates
        )
    if aggregate not in aggregates['global_scope']:
        aggregates['global_scope'].append(aggregate)
    return (
        GlobalAggregateCondition(
            aggregate=aggregate,
            operator=operator,
            value=numeric_value
        ),
        aggregates
    )


###########################################################################
# ORDER BY Clause Parsing
###########################################################################
def parse_order_by_clause(order_by_clause: str | None, number_of_select_grouping_attributes: int):
    if order_by_clause == None:
        return 0
    try:
        order_value = int(order_by_clause.strip())  
    except ValueError:
        raise ParsingError(ParsingErrorType.ORDER_BY_CLAUSE, f"Invalid value: '{order_by_clause}'")
    if order_value < 1 or order_value > number_of_select_grouping_attributes:
        raise ParsingError(ParsingErrorType.ORDER_BY_CLAUSE, f"{order_by_clause.strip()} out of range of the {number_of_select_grouping_attributes} grouping attributes provided in the select clause.")
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
    
    raise ParsingError(error_type, f"Invalid aggregate: '{aggregate}'\nAggregate must be in the format 'column.function' or 'group.column.function'")


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
def _split_condition(condition: str) -> tuple[str, str, str] | None:
    CONDITIONAL_OPERATORS = ['>=', '<=', '!=', '==', '>', '<', '=']
    in_single = False
    in_double = False

    for i in range(len(condition)):
        char = condition[i]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if not in_single and not in_double:
            for op in sorted(CONDITIONAL_OPERATORS, key=len, reverse=True):
                if condition[i:i+len(op)] == op:
                    return condition[:i].strip(), op, condition[i+len(op):].strip()

    return None


def _has_wrapping_parenthesis(condition: str) -> bool:
    condition = condition.strip()
    if not (condition.startswith('(') and condition.endswith(')')):
        return False

    paren_level = 0
    in_single_quote = False
    in_double_quote = False
    for i, char in enumerate(condition):
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if in_single_quote or in_double_quote:
            continue

        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1

        if paren_level == 0 and i < len(condition) - 1:
            return False

    return paren_level == 0


def _split_by_logical_operator(condition: str, operator: LogicalOperator) -> list[str]:
    parts = []
    current = ''
    paren_level = 0
    in_single_quote = False
    in_double_quote = False
    i = 0

    while i < len(condition):
        char = condition[i]

        # Toggle quote state
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        # Track parentheses only when not in quotes
        if not in_single_quote and not in_double_quote:
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1

        # Check for the logical operator (e.g., AND, OR) when outside parens and quotes
        if (
            not in_single_quote and not in_double_quote and paren_level == 0 and
            char == ' ' and
            i + 1 + len(operator.value) <= len(condition) and
            condition[i + 1:i + 1 + len(operator.value)].lower() == operator.value.lower() and
            (i + 1 + len(operator.value) == len(condition) or condition[i + 1 + len(operator.value)] in (' ', ')'))
        ):
            parts.append(current.strip())
            current = ''
            i += len(operator.value) + 1
        else:
            current += char
            i += 1

    if current.strip():
        parts.append(current.strip())
    return parts



   
        

        