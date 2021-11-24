from pandas import DataFrame

dataset = {
    'target': DataFrame([
        [1, 'foo', 'oof'],
        [2, 'bar', 'rab'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog']],
        columns=['id', 'c1', 'c2']
    ),
    'empty_source': DataFrame([],
        columns=['id', 'c1', 'c2', 'diff']
    ).astype(dtype={'id': 'int64', 'c1': 'string', 'c2': 'string', 'diff': 'string'}),
    'empty_expected': DataFrame([
        [1, 'foo', 'oof'],
        [2, 'bar', 'rab'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog']],
        columns=['id', 'c1', 'c2']
    ),
    'not_empty_source': DataFrame([
        [5, 'gar', 'rag', 'I'],
        [6, 'gaz', 'zag', 'I'],
        [3, 'baz', 'baz', 'U'],
        [1, None, None, 'D']],
        columns=['id', 'c1', 'c2', 'diff']
    ),
    'not_empty_expected': DataFrame([
        [2, 'bar', 'rab'],
        [3, 'baz', 'baz'],
        [4, 'goo', 'oog'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
}
