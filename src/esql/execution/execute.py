from src.esql.parser.types import ParsedQuery




def execute(parsed_query: ParsedQuery):
    
    
    # # Extract parts from the parsed query.
    # select_struct = processed_query['select_clause']
    # grouping_attributes = select_struct['columns']
    # aggregate_groups = processed_query['aggregate_groups']
    # where_conditions = processed_query['where_conditions']
    # such_that_conditions = processed_query['such_that_conditions']
    # having_conditions = processed_query['having_conditions']
    # order_by = processed_query['order_by']
    # datatable = processed_query['datatable']
    # columns = processed_query['columns']
    # column_indexes = processed_query['column_indexes']
    pd_datatable = parsed_query.data
    column_dtypes = pd_datatable.dtypes.to_dict()
    columns = pd_datatable.columns.tolist()
    column_indices = { column: index for index, column in enumerate(columns) }
    databale = pd_datatable.values.tolist()


    
    # Set the global DATATABLE (including converting date strings to datetime.date).
    # algorithms.set_datatable_information(datatable, column_indexes, columns)
    
    # --- Global WHERE Filtering ---
    if where_conditions:
        filtered_rows = [
            row for row in algorithms.DATATABLE 
            if algorithms.evaluate_condition(where_conditions, row)
        ]
    else:
        filtered_rows = algorithms.DATATABLE
    
    # (Optional debug: Uncomment to verify number of rows after filtering)
    # print(f"DEBUG: Rows before filtering: {len(algorithms.DATATABLE)}; after filtering: {len(filtered_rows)}")
    
    # Reset the global DATATABLE to the filtered rows.
    #algorithms.set_datatable(filtered_rows)
    
    # --- Build the H Table ---
    hTable = algorithms.build_hTable(
        grouping_attributes,
        processed_query['aggregate_functions'],
        aggregate_groups,
        such_that_conditions,
        having_conditions,
        where_conditions=None  # Filtering already applied
    )
    
    # (Optional debug: Uncomment to verify number of groups formed)
    # print(f"DEBUG: Number of groups in H Table: {len(hTable)}")
    
    # --- Projection ---
    # Project both the grouping attributes and the aggregate columns.
    # (Assumes that your select_struct contains an "aggregates" dictionary.)
    result = algorithms.project_select_attributes(
        hTable,
        grouping_attributes,
        select_struct['aggregates']
    )
    
    # --- Ordering ---
    result = algorithms.order_by_sort(result, order_by, grouping_attributes)
    
    return result