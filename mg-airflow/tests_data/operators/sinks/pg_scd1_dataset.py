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
        columns=['id', 'c1', 'c2']
    ).astype(dtype={'id':'int64', 'c1': 'string', 'c2': 'string'}),
    'empty_expected': DataFrame([
        [1, 'foo', 'oof'],
        [2, 'bar', 'rab'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog']],
        columns=['id', 'c1', 'c2']
    ),
    'insert_only_source': DataFrame([
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'insert_only_expected': DataFrame([
        [1, 'foo', 'oof'],
        [2, 'bar', 'rab'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'delete_insert_source': DataFrame([
        [1, 'foo', 'foo'],
        [2, 'bar', 'bar'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'delete_insert_expected': DataFrame([
        [1, 'foo', 'foo'],
        [2, 'bar', 'bar'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'deleted_flg_column_source': DataFrame([
        [1, 'foo', 'foo', 0],
        [2, 'bar', 'bar', 0],
        [3, 'baz', 'zab', 1],
        [5, 'gar', 'rag', 0],
        [6, 'gaz', 'zag', 0]],
        columns=['id', 'c1', 'c2', 'del_flg']
    ),
    'deleted_flg_column_expected': DataFrame([
        [1, 'foo', 'foo'],
        [2, 'bar', 'bar'],
        [4, 'goo', 'oog'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'process_deletions_source': DataFrame([
        [3, 'baz', 'zab'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'process_deletions_expected': DataFrame([
        [3, 'baz', 'zab'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'process_existing_records_source': DataFrame([
        [1, 'foo', 'foo'],
        [2, 'bar', 'bar'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
    'process_existing_records_expected': DataFrame([
        [1, 'foo', 'foo'],
        [2, 'bar', 'bar'],
        [3, 'baz', 'zab'],
        [4, 'goo', 'oog'],
        [5, 'gar', 'rag'],
        [6, 'gaz', 'zag']],
        columns=['id', 'c1', 'c2']
    ),
}
