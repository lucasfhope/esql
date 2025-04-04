from typing import TypedDict, Union, Literal, List, Dict, Tuple
from enum import Enum
from datetime import date


'''
PARSING ERROR
'''

#    aggregates = {'global': [], 'group_specific': []}
    
class GlobalAggregate(TypedDict):
    column: str
    function: str

class GroupAggregate(GlobalAggregate):
    group: str

class AggregatesDict(TypedDict):
    global_scope: List[GlobalAggregate]
    group_specific: List[GroupAggregate]


class ParsedSelectClause(TypedDict):
    columns: List[str]
    aggregates: AggregatesDict




###################
# Conditions
###################

class LogicalOperator(Enum):
    AND = "and"
    OR = "or"
    NOT = "not"

class SimpleCondition(TypedDict):
    column: str
    operator: str
    value: Union[float, bool, str, date]
    is_emf: bool  # EMF is when the comparison value is based on the entry value of the column.                      

class CompoundCondition(TypedDict):
    operator: Literal[LogicalOperator.AND, LogicalOperator.OR]
    conditions: List['ParsedWhereClause']

class NotCondition(TypedDict):
    operator: Literal[LogicalOperator.NOT]
    condition: 'ParsedWhereClause'
    
ParsedWhereClause = (
    SimpleCondition |
    CompoundCondition |
    NotCondition
)


class SimpleGroupCondition(SimpleCondition):
    group: str

class CompoundGroupCondition(CompoundCondition):
    conditions: List['ParsedSuchThatClause']

class NotGroupCondition(NotCondition):
    condition: 'ParsedSuchThatClause'

ParsedSuchThatClause = (
    SimpleGroupCondition |
    CompoundGroupCondition |
    NotGroupCondition
)



class GlobalAggregateCondition(TypedDict):
    aggregate: GlobalAggregate
    operator: str
    value: float   

class GroupAggregateCondition(GlobalAggregateCondition):
    aggregate: GroupAggregate

# Compound condition for HAVING clause using AND/OR.
class CompoundAggregateCondition(TypedDict):
    operator: List['ConditionType']
    conditions: List["ParsedHavingClause"]

# NOT condition that wraps a single condition.
class NotAggregateCondition(TypedDict):
    operator: Literal[LogicalOperator.NOT]
    condition: "ParsedHavingClause"

# Union of all possible HAVING clause condition types.
ParsedHavingClause = (
    GlobalAggregateCondition |
    GroupAggregateCondition |
    CompoundAggregateCondition |
    NotAggregateCondition
)











class ParsedQuery(TypedDict):
    select: ParsedSelectClause
    over_groups: list[str]
    where: ParsedWhereClause








