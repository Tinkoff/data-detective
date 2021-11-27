from pandas import DataFrame


def decode_response(_context: dict, s3_dump: DataFrame) -> DataFrame:
    """Decode the DataFrame

    @param _context: Context
    @param s3_dump: ['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner', 'response']
    @return: ['test', 'test1']]
    """
    result = DataFrame()
    result['test'] = s3_dump['key'].str.len()
    result['test1'] = s3_dump['response'].str.decode('utf8').str.len()
    return result[['test', 'test1']]


def rename_path(_context: dict, s3_dump: DataFrame) -> DataFrame:
    """Change path

    @param _context: Context
    @param s3_dump: ['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner', 'response']
    @return: ['path', 'response']
    """
    result = s3_dump[['response']]
    result.loc[:, 'path'] = s3_dump['key'].apply(lambda x: x[::-1])  # reverse
    return result[['path', 'response']]
