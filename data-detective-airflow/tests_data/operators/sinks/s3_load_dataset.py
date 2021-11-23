from numpy import nan
from pandas import DataFrame

dataset = DataFrame(
    [
        ['load1.txt', b'dd_airflow\n', nan],
        ['load2.txt', b'teditedi\n', {'ContentType': 'image/svg+xml', 'Metadata': {'Content-Type': 'image/svg+xml'}}],
    ],
    columns=['path', 'data', 'metadata']
)
