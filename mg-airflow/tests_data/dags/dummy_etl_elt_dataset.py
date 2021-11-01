from pandas import DataFrame

dataset = {
    'elt': DataFrame(
        [
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
    'etl': DataFrame(
        [
            [1, 3],
        ],
        columns=['test', 'test1']
    ),
    'sink': DataFrame(
        [
            [1, 3],
        ],
        columns=['test', 'test1']
    ),
}
