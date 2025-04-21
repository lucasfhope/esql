from src.esql.execution.algorithms import order_by_sort


def test_order_by_sort():
    grouping_attributes = ["cust", "prod"]
    order_by_0 = [ 
        { "cust": "Wally", "prod": "Butter", "round": 480.41, "sum": 55727 },
        { "cust": "Boo", "prod": "Cherry", "round": 486.66, "sum": 48666 },
        { "cust": "Helen", "prod": "Butter", "round": 464.56, "sum": 48779 },
        { "cust": "Claire", "prod": "Butter", "round": 519.05, "sum": 67995 },
        { "cust": "Wally", "prod": "Apple", "round": 534.51, "sum": 58262 },
        { "cust": "Mia", "prod": "Butter", "round": 502.17, "sum": 54234 },
        { "cust": "Claire", "prod": "Ham", "round": 507.43, "sum": 58354 },
        { "cust": "Boo", "prod": "Ice", "round": 472.79, "sum": 45861 },
        { "cust": "Mia", "prod": "Cherry", "round": 478.53, "sum": 51203 },
        { "cust": "Helen", "prod": "Ice", "round": 515.34, "sum": 60295 },
        { "cust": "Wally", "prod": "Ham", "round": 533.85, "sum": 59257 },
        { "cust": "Helen", "prod": "Cherry", "round": 493.63, "sum": 61704 },
        { "cust": "Wally", "prod": "Cherry", "round": 527.54, "sum": 63832 },
        { "cust": "Mia", "prod": "Grapes", "round": 444.25, "sum": 45758 }
    ]

    order_by_1 = [ 
        { "cust": "Boo", "prod": "Cherry", "round": 486.66, "sum": 48666 },
        { "cust": "Boo", "prod": "Ice", "round": 472.79, "sum": 45861 },
        { "cust": "Claire", "prod": "Butter", "round": 519.05, "sum": 67995 },
        { "cust": "Claire", "prod": "Ham", "round": 507.43, "sum": 58354 },
        { "cust": "Helen", "prod": "Butter", "round": 464.56, "sum": 48779 },
        { "cust": "Helen", "prod": "Ice", "round": 515.34, "sum": 60295 },
        { "cust": "Helen", "prod": "Cherry", "round": 493.63, "sum": 61704 },
        { "cust": "Mia", "prod": "Butter", "round": 502.17, "sum": 54234 },
        { "cust": "Mia", "prod": "Cherry", "round": 478.53, "sum": 51203 },
        { "cust": "Mia", "prod": "Grapes", "round": 444.25, "sum": 45758 },
        { "cust": "Wally", "prod": "Butter", "round": 480.41, "sum": 55727 },
        { "cust": "Wally", "prod": "Apple", "round": 534.51, "sum": 58262 },
        { "cust": "Wally", "prod": "Ham", "round": 533.85, "sum": 59257 },
        { "cust": "Wally", "prod": "Cherry", "round": 527.54, "sum": 63832 }
    ]
        

    order_by_2 = [ 
        { "cust": "Boo", "prod": "Cherry", "round": 486.66, "sum": 48666 },
        { "cust": "Boo", "prod": "Ice", "round": 472.79, "sum": 45861 },
        { "cust": "Claire", "prod": "Butter", "round": 519.05, "sum": 67995 },
        { "cust": "Claire", "prod": "Ham", "round": 507.43, "sum": 58354 },
        { "cust": "Helen", "prod": "Butter", "round": 464.56, "sum": 48779 },
        { "cust": "Helen", "prod": "Cherry", "round": 493.63, "sum": 61704 },
        { "cust": "Helen", "prod": "Ice", "round": 515.34, "sum": 60295 },
        { "cust": "Mia", "prod": "Butter", "round": 502.17, "sum": 54234 },
        { "cust": "Mia", "prod": "Cherry", "round": 478.53, "sum": 51203 },
        { "cust": "Mia", "prod": "Grapes", "round": 444.25, "sum": 45758 },
        { "cust": "Wally", "prod": "Apple", "round": 534.51, "sum": 58262 },
        { "cust": "Wally", "prod": "Butter", "round": 480.41, "sum": 55727 },
        { "cust": "Wally", "prod": "Cherry", "round": 527.54, "sum": 63832 },
        { "cust": "Wally", "prod": "Ham", "round": 533.85, "sum": 59257 }
    ]

    assert order_by_sort(
        projected_table=order_by_0,
        order_by=0,
        grouping_attributes=grouping_attributes
    ) == order_by_0

    assert order_by_sort(
        projected_table=order_by_0,
        order_by=1,
        grouping_attributes=grouping_attributes
    ) == order_by_1

    assert order_by_sort(
        projected_table=order_by_0,
        order_by=2,
        grouping_attributes=grouping_attributes
    ) == order_by_2

