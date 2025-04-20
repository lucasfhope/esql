import pandas as pd
from enum import Enum
from datetime import date
from typing import TypedDict, Union, Literal, List, Dict, Tuple


class GlobalAggregate(TypedDict):
    column: str
    function: str
    def __eq__(self, other):
        return self.column == other.column and self.function == other.function

class GroupAggregate(GlobalAggregate):
    group: str
    def __eq__(self, other):
        return self.group == other.group and self.column == other.column and self.function == other.function

class AggregatesDict(TypedDict):
    global_scope: List[GlobalAggregate]
    group_specific: List[GroupAggregate]
    


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


class SimpleGroupCondition(SimpleCondition):
    group: str

class CompoundGroupCondition(CompoundCondition):
    conditions: List['ParsedSuchThatClause']

class NotGroupCondition(NotCondition):
    condition: 'ParsedSuchThatClause'


class GlobalAggregateCondition(TypedDict):
    aggregate: GlobalAggregate
    operator: str
    value: float   

class GroupAggregateCondition(GlobalAggregateCondition):
    aggregate: GroupAggregate

class CompoundAggregateCondition(TypedDict):
    operator: List['ConditionType']
    conditions: List['ParsedHavingClause']

class NotAggregateCondition(TypedDict):
    operator: Literal[LogicalOperator.NOT]
    condition: 'ParsedHavingClause'


class ParsedSelectClause(TypedDict):
    grouping_attributes: List[str]
    aggregates: AggregatesDict
    aggregate_keys_in_order: List[str]

ParsedWhereClause = (
    SimpleCondition |
    CompoundCondition |
    NotCondition
)

ParsedSuchThatSection = (
    SimpleGroupCondition |
    CompoundGroupCondition |
    NotGroupCondition
)
ParsedSuchThatClause = List[ParsedSuchThatSection]

ParsedHavingClause = (
    GlobalAggregateCondition |
    GroupAggregateCondition |
    CompoundAggregateCondition |
    NotAggregateCondition
)

class ParsedQuery(TypedDict):
    data: pd.DataFrame
    select: ParsedSelectClause
    over: List[str] | None
    where: ParsedWhereClause | None
    such_that: ParsedSuchThatSection | None
    having: ParsedHavingClause | None
    order_by: int
    aggregates: AggregatesDict










