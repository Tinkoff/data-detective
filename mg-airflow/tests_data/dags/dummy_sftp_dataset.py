from pandas import DataFrame

dataset = {
    'test1': b'#!/bin/bash\\nDUMMY=echo\n'
    ,
    'put_data_to_df': DataFrame(
        [
            ['#!/bin/bash\\nDUMMY=echo\n'],
        ],
        columns=['path']
    ),
}
