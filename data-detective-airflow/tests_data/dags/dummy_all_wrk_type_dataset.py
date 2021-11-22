from pandas import DataFrame

dataset = {
    't1': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
    't2': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
    't3': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
    't4': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
    't5': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
    'test_pg_sink': DataFrame(
        [
            ['aa', 'ab', 'ac'],
            ['ba', 'bb', 'bc'],
            ['ca', 'cb', 'cc']
        ],
        columns=['c1', 'c2', 'c3']),
}