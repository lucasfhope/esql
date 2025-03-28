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
    datatype: float

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

class CompoundCondition(TypedDict):
    operator: Literal[LogicalOperator.AND, LogicalOperator.OR]
    conditions: List['ConditionType']

class NotCondition(TypedDict):
    operator: Literal[LogicalOperator.NOT]
    condition: 'ConditionType'

ConditionType = SimpleCondition | CompoundCondition | NotCondition
ParsedWhereClause = ConditionType


class SimpleGroupCondition(SimpleCondition):
    group: str

class CompoundGroupCondition(CompoundCondition):
    conditions: List['ParsedSuchThatClause']
    group: str

class NotGroupCondition(NotCondition):
    condition: 'ParsedSuchThatClause'

ParsedSuchThatClause = Union[SimpleGroupCondition, CompoundGroupCondition, NotGroupCondition]



class GlobalHavingCondition(TypedDict):
    column: str
    function: str
    operator: str
    value: float

class GroupHavingCondition(GlobalHavingCondition):
    group: str

# Compound condition for HAVING clause using AND/OR.
class CompoundHavingCondition(TypedDict):
    operator: Literal["AND", "OR"]
    conditions: List["ParsedHavingClause"]

# NOT condition that wraps a single condition.
class NotHavingCondition(TypedDict):
    operator: Literal["NOT"]
    condition: "ParsedHavingClause"

# Union of all possible HAVING clause condition types.
ParsedHavingClause = Union[
    GlobalHavingCondition,
    GroupHavingCondition,
    CompoundHavingCondition,
    NotHavingCondition
]











class ParsedQuery(TypedDict):
    select: ParsedSelectClause
    over_groups: list[str]
    where: ParsedWhereClause








