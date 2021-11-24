---
sidebar_position: 6
---

# DAG testing

Our team set a goal to achieve convenient testing of datasets in pipelines.

To achieve the goal , it is necessary
* save datasets in a readable format
* regenerate datasets
* configure test environments, prepare input data

We have developed a solution that saves datasets in the form of JSON.
`pandas.DataFrame.to_json` allows saving json to a file in a readable form.
Each json element is saved in a new line. When modifying DAGs, the developer regenerates json, and git shows readable diff. This allows the developer to evaluate changes by the task.

DAGs in mg-airflow consist of operators. Operators take input data from external sources or works.
The TBaseOperator defines the read_result method for reading the final dataset. Usually the operator saves the data in work.
PgSCD1 modifies the data in the target table. For testing, data is needed from the target, and not from the temporary table in pg-work,
so the read_result method is redefined in PgSCD1.

It is important to prepare an external environment as similar as possible to a grocery one. docker-compose allows you to organize such an environment.
It is possible to initialize databases and raise a web server that will respond in a similar way and use mockups from pytest-mock.

Let's take a closer look at the example of a dummy DAG:
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

JSONPandasDataset is a class that creates a dictionary of the type `dict[task_name, DataFrame]`.
When calling `dataset[task_name]`, a json file is read, which is converted to a DataFrame.

Writing to JSONPandasDataset then proceeds as follows. The dataset key is assigned a DataFrame.
Then the DataFrame is converted to json and saved to a file called task in json and md formats.
The comparison takes place using json and the markdown format allows to see the diff for even small changes.

Then the data is tested using py test.
If there is a non-empty value for the GEN_DATASET variable in the environment variables, the test data generation mode is started.
If GEN_DATASET is empty, then testing is started in the test_task function.
The value of the GEN_DATASET environment variable is defined in the is_gen_dataset_mode function.
Depending on its value, the decorator `@pytest.mark.skip if` mutually excludes the mode of testing and creating reference data.

Thus,
* go into the container and run the bash command shell `docker-compose exec app bash`
* enable test data generation mode `export GEN_DATASET=any`
* run pytest `pytest tests/dags/test_dummy.py` - test data for dataset will be recreated
* turn off the test data generation mode `export GEN_DATASET=`
* run pytest `pytest tests/dags/test_dummy.py` again - tests will be run

If PyCharm Professional is used, then the GEN_DATASET value can be registered in the Environment Variables
in the startup configuration and run testing/generation through the application, not the console.

`@pytest.mark.parametrize('task', dag.tasks)` - is a decorator that runs a test for each DAG task.

run_and_assert_task starts only one task. Input datasets for the task are inserted into the function.
Next, a task is launched, transformations are performed, the output dataset is compared with the reference one.

run_and_gen_ds saves reference data along the transmitted path.

### Notes

* DataFrame and other structures in python can contain binary data.
Binary data cannot be stored in JSON. In such cases, it is necessary to use decode/encode or abandon JSONPandasDataset.
* run_and_assert_task uses xor (exclusive or) for comparison and allows not to sort the dataset.
