# About testing

Нами была поставлена цель добиться удобного тестирование датасетов в пайплайнах.

Для этого нужно
* уметь сохранять датасеты в читаемом формате
* уметь перегенерировать датасеты
* настраивать тестовые окружения, готовить входные данные

Мы разработали решение, сохраняющее датасеты в виде JSON.
`pandas.DataFrame.to_json` позволяет сохранить json в файл в читаемом виде.
Каждый элемент json сохраняется в новой строке. При доработках DAG-ов разработчик перегенерирует json.
git показывает читаемые diff. Это позволяет оценивать изменения по задаче.

DAG-и в mg-airflow состоят из операторов. Операторы берут входные данные из внешних источников или ворков.
В TBaseOperator определен метод read_result для чтения итогового датасета. Обычно оператор сохраняет данные в work.
PgSCD1 изменяет данные в target таблице. Для тестирования нужны данные из таргета, а не из временной таблицы в pg-work, 
поэтому в PgSCD1 переопределен метод read_result.

Важно подготовить внешнее окружение максимально похожее на продуктовое. docker-compose позволяет организовать такое окружение.
Можно инициализировать базы данных и поднимать веб-сервер, который будет отвечать похожим образом.
Также можно использовать моки из pytest-mock.

Рассмотрим детальнее на примере DAG-а dummy.
```python
dag_name = 'dummy'
dag = generate_dag(dag_dir=f'{settings.DAGS_FOLDER}/dags/{dag_name}')
dataset = JSONPandasDataset(f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')

gen_dataset = is_gen_dataset_mode()

@pytest.mark.skipif(condition=gen_dataset, reason='Gen dataset')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_task(task, mocker, setup_tables):
    run_and_assert_task(task=task, dataset=dataset, mocker=mocker)


@pytest.mark.skipif(condition=(not gen_dataset), reason='Gen dataset')
@pytest.mark.parametrize(
    'task', dag.tasks
)
def test_gen_tests_data(task, mocker, setup_tables):
    run_and_gen_ds(task, f'{settings.AIRFLOW_HOME}/tests_data/dags/{dag_name}')
```

JSONPandasDataset - класс, который создает словарь вида `dict[task_name, DataFrame]`.
При вызове `dataset[task_name]` считывается файл с json, который преобразовывается в DataFrame.

Запись в JSONPandasDataset происходит следующим образом. Ключу dataset присваивается DataFrame.
DataFrame преобразовывается в json и сохраняется в файл с названием таска в форматах json и md.
Сравнение происходит по json. Формат markdown позволяет увидеть diff для небольших изменений.

Тестируем с помощью pytest.
Если в переменных среды есть непустое значение для переменной GEN_DATASET, то запускается режим генерации тестовых данных.
Если GEN_DATASET пустая, то запускается тестирование в функции test_task.
Значение переменной среды GEN_DATASET определяется в функции is_gen_dataset_mode.
В зависимости от ее значения декоратор `@pytest.mark.skipif` взаимоисключает режим тестирования и создания эталонных данных. 

Таким образом, 
* заходим в контейнер и запускаем командную оболочку bash `docker-compose exec app bash`
* включить режим генерации тестовых данных `export GEN_DATASET=any`
* запускаем pytest `pytest tests/dags/test_dummy.py` - будут пересозданы тестовые данные для dataset
* выключить режим генерации тестовых данных `export GEN_DATASET=`
* запускаем снова pytest `pytest tests/dags/test_dummy.py` - будут запущены тесты

Если вы используете PyCharm Professional, то значение GEN_DATASET можно прописать в Environment Variables
в конфигурации запуска и запускать тестирование/генерацию через приложение, а не консоль. 

`@pytest.mark.parametrize('task', dag.tasks)` - декоратор, который запускает тест для каждого таска DAG-а.

run_and_assert_task запускает только один таск. В функции подставляются входные датасеты для таска.
Запускается таск, выполняются преобразования, сравнивается выходной датасет с эталонным.

run_and_gen_ds сохраняет эталонные данные по передаваемому пути.

### Заметки

* DataFrame и другие структуры в python могут содержать бинарные данные.
В JSON нельзя сохранять бинарные данные. В таких случаях нужно применять decode/encode либо отказаться от JSONPandasDataset.
* run_and_assert_task использует xor (исключающее или) для сравнения. Это позволяет не сортировать датасет.
