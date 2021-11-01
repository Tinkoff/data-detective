# Создание нового оператора

* Обязательно отнаследоваться от TBaseOperator.

```python
class MyTransform(TBaseOperator):

    def __init__(self, source: List[str], on_columns=None, how='left', **kwargs):
        super().__init__(**kwargs)
        self.source = source
    ...
```
* Если в конструкторе оператора есть параметр - функция, то называть ее нужно с постфиксом `*_callable`.

Например:
```python
class MyTransform(TBaseOperator):

    def __init__(self, py_callable,  **kwargs):
    ...
```
