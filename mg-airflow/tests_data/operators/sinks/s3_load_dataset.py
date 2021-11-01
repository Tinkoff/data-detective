from numpy import nan
from pandas import DataFrame

dataset = DataFrame(
    [
        ['loadtedi.txt', b'mg_airflow\n', nan],
        ['loadteditedi.txt', b'teditedi\n', {'ContentType': 'image/svg+xml', 'Metadata': {'Content-Type': 'image/svg+xml'}}],
    ],
    columns=['path', 'data', 'metadata']
)
