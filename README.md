# FIBER

The fiber package.


* Free software: MIT license

## Installation

From source:

```
git clone https://github.com/hpi-dhc/fiber && \
cd fiber && \
pip install -e .
```

## Quickstart

For a quick start, checkout [this iPython notebook](/notebooks/heart-surgery-demo.ipynb). We demonstrate the most important [Features](#features) in there.

## Configuration of the database connection

Check out `.env-example` to setup the HANA or MySQL connection.

## Features

### Cohort

#### Basic Functions:

- `occurs(target, event)`
        All target occurrences in relation to an event.
- `values_for(target, event)`
        All target values in relation to an event.

For each event, all relevant target entries are returned with a relative timedelta in days.

#### Postprocessing Functions:

- `has_co_occurrence(df, time_windows)`
        Binary occurrence for different time windows relative to the event.
- `aggregate_values(df, time_windows)`
        Aggregate values for different time windows relative to the event.

#### Convenience Functions:

Wrappers around `has_co_occurrence` for frequently used

- `has_onset(time_windows=[(0, 1), (0, 7), (0, 14), (0, 28)])`
- `has_precondition(time_windows=[(-inf, 0)])`

### Stores

#### YAMLStore

* Retrieve lists of codes for ICD-10, ICD-9 and other schemes. Can be used for example for diagnosis, procedures or vital signs.
    ```python
    import fiber.storage.yaml as fiber_yaml
    fiber_yaml.get_condition(Procedure, 'heart surgery', ['ICD-10', 'ICD-9'])
    ```
