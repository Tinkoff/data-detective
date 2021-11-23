from pandas import DataFrame
from pandas._libs.tslibs.timestamps import Timestamp

dataset = {
    'list_bucket': DataFrame(
        [
            ['dd-airflow', Timestamp('2020-11-09 08:29:03+0000', tz='tzlocal()'), '"e9ce7bfc6c70c80bb03e168fb64f925b"', 11, 'STANDARD', {'DisplayName': 'webfile', 'ID': '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a'}],
            ['dd-airflow.txt', Timestamp('2020-11-09 08:29:03+0000', tz='tzlocal()'), '"bcb6d17ea497aec5b15cc12eeb0d2f49"', 15, 'STANDARD', {'DisplayName': 'webfile', 'ID': '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a'}]
        ],
        columns=['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner']),
    's3_dump': DataFrame(
        [
            ['dd-airflow', Timestamp('2020-11-09 08:29:03+0000', tz='tzlocal()'), '"e9ce7bfc6c70c80bb03e168fb64f925b"', 11, 'STANDARD', {'DisplayName': 'webfile', 'ID': '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a'}, b'dd-airflow\n'],
            ['dd-airflow.txt', Timestamp('2020-11-09 08:29:03+0000', tz='tzlocal()'), '"bcb6d17ea497aec5b15cc12eeb0d2f49"', 15, 'STANDARD', {'DisplayName': 'webfile', 'ID': '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a'}, b'dd-airflow.txt\n']
        ],
        columns=['key', 'lastmodified', 'etag', 'size', 'storageclass', 'owner', 'response']),
    'decode_response': DataFrame(
        [
            [10, 11],
            [14, 15]
        ],
        columns=['test', 'test1']),
    'pg_sink': DataFrame(
        [
            [10, 11],
            [14, 15]
        ],
        columns=['test', 'test1']),
    'rename_path': DataFrame(
        [
            ['wolfria-dd', b'dd-airflow\n'],
            ['txt.wolfria-dd', b'dd-airflow.txt\n']
        ],
        columns=['path', 'response']),
}
