# Frameworks for working with datasets

## pandas

Docs - https://pandas.pydata.org/docs/

The most popular solution for working with tabular data.
Allows you to transform data in multiple ways: grouping, filtering, transformations.
There are ways to work with a large number of formats: csv, avro, json, markdown and others.

The main concepts are Series and DataFrame.
When working with a DataFrame, it should be borne in mind that the data in it is stored vertically in columns.
As a result, it is unwise to iterate over all columns at once.

When working with etl, a common problem is the appearance of NotANumber (NaN, NA, nan). Loading to external sources falls on them.
It is usually necessary to clean the NaN before the final uploads. NaN is also usually useful in mathematical calculations.

## petl 

Docs - https://petl.readthedocs.io/en/stable/

petl works with abstractions over a two-dimensional TupleOfTuples or ListOfList structure.
Transformations take place in a functional style.
All transformations can be performed in one pass through the dataset.
The exceptions are: sorting, grouping, finding unique records.

This tool has been developing for a long time and has all the necessary transformations for ETL.

It has basic capabilities for working with other formats, so it is preferable to use pandas for conversion tasks to another format.

It is considered optimal to work with datasets up to 1 gigabyte. For large volumes, it is better to take a DataFrame.
DataFrame stores data more compactly than TupleOfTuples.
