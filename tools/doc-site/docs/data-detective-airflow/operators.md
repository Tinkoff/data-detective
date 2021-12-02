---
id: creating-operator
---

# Creating a new operator

* Must be inherited from TBaseOperator.

```python
class MyTransform(TBaseOperator):

    def __init__(self, source: List[str], on_columns=None, how='left', **kwargs):
        super().__init__(**kwargs)
        self.source = source
    ...
```
* If there is a function parameter in the operator's constructor, then it must be called with the postfix `* _callable`.

For example:
```python
class MyTransform(TBaseOperator):

    def __init__(self, py_callable,  **kwargs):
    ...
```
