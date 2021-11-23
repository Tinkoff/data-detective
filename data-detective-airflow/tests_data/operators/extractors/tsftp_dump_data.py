from pandas import DataFrame


jobs_df = DataFrame([
    ['DDS LOAD 1', 1, 'GP'],
    ['DDS LOAD 2', 2, 'GP'],
    ['DV LOAD 1', 3, 'GP'],
    ['MOV 1', 4, 'ORA']],
    columns=['job', 'id', 'target_system'])


users_df = DataFrame([
    ['USER 1', 11, 'XXXX'],
    ['USER 2', 22, 'XXXXX']],
    columns=['user', 'id', 'password'])


cyrillic_df = DataFrame([
    ['ODD', 'Jobs with the "ODD LOAD" prefix unload data into the _odd schema.'],
    ['DDS', 'Джоб с префиксом "DDS LOAD" выгружает данные в _dds схему.']],
    columns=['layer', 'description'])


special_df = DataFrame([
    [111, 'Это просто символы: /@$%^&*()\n[]\t{}|\\+_-=#№!`~'],
    [222, '^#^']],
    columns=['key', 'data'])
