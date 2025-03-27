import pandas as pd
from pandas.api.extensions import register_dataframe_accessor
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype

from src.esql.parser.parse import get_processed_query

@register_dataframe_accessor("esql")
class ESQLAccessor:
    def __init__(self, df: pd.DataFrame):
        self.df = _enforce_allowed_dtypes(df)


    def query(self, query: str) -> pd.DataFrame:
        # Parse the query
        query_struct = get_processed_query(self.df, query)

        # Execute the query (make sure converted back into df)




def _enforce_allowed_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Convert DataFrame columns so that each column's dtype is one of:
      - "string" for textual data
    - bool for boolean data
      - datetime64[ns] for date/time data
      - int or float for numeric data
      
    For numeric columns, if they're already integer or float, they are left unchanged.
    Any columns that don't match the allowed types (except bool and datetime) 
    will be converted to the "string" dtype.
    
    Parameters:
        df: The input DataFrame.
    
    Returns:
        pd.DataFrame: A new DataFrame with enforced dtypes.
    '''

    df = df.copy()
    
    for col in df.columns:
        current_dtype = df[col].dtype
        
        if pd.api.types.is_string_dtype(current_dtype):
            df[col] = df[col].astype("string")
        elif pd.api.types.is_bool_dtype(current_dtype):
            pass
        elif pd.api.types.is_datetime64_any_dtype(current_dtype):
            pass
        elif pd.api.types.is_numeric_dtype(current_dtype):
            if not (pd.api.types.is_integer_dtype(current_dtype) or pd.api.types.is_float_dtype(current_dtype)):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            # For any other type (object, categorical, ...) convert to string.
            df[col] = df[col].astype("string")
    
    return df
       