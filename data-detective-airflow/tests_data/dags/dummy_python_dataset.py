from pandas import DataFrame

dataset = {
    'test1': DataFrame(
        [
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
    'test2': DataFrame(
        [
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
    'transform': DataFrame(
        [
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
    'append_all': DataFrame(
        [
            [1, 1],
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
    'sink': DataFrame(
        [
            [1, 1],
            [1, 1],
        ],
        columns=['test', 'test1']
    ),
}
