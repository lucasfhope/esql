import pytest
import numpy as np
import pandas as pd

from src.esql.main import _enforce_allowed_dtypes
from src.esql.parser.parse import _prepare_query, get_parsed_query


@pytest.fixture
def data() -> dict[str, np.dtype]: 
    data = _enforce_allowed_dtypes(
        pd.read_csv(
            'public/data/sales.csv'
        )
    )
    return data


def test_prepare_query_returns_expected_structure():
    query = 'SELECT cust,         prod,         1.quant.sum OVER g1,g2,g3      WHERE cust = "DAN" and month = 1 or prod =   "APLEHSVCDGhsfjadmg_hgdoe3v¡=3h8d" Such thAt g3.prod = \'bceL;lwhan\',g2.state="NY" HAvING g1.quant.avg > 0.5 orDer by     3'
    expected = 'select cust, prod, 1.quant.sum over g1,g2,g3 where cust = "DAN" and month = 1 or prod = "APLEHSVCDGhsfjadmg_hgdoe3v¡=3h8d" such that g3.prod = \'bceL;lwhan\',g2.state="NY" having g1.quant.avg > 0.5 order by 3'
    result = _prepare_query(query)
    assert result == expected

def test_get_parsed_query_returns_the_expected_structure(data: pd.DataFrame):
    parsedQuery = get_parsed_query(
        data=data,
        query='SELECT cust,         prod,         g1.quant.sum OVER g1,g2,g3      WHERE cust = "DAN" and month = 1 or prod =   "APLEHSVCDGhsfjadmg_hgdoe3v¡=3h8d" Such thAt g3.prod = \'bceL;lwhan\',g2.state="NY" HAvING g1.quant.avg > 0.5 orDer by     1'
    )
    assert parsedQuery









if __name__ == '__main__':
    pytest.main()