from fiber.condition import (
    Diagnosis,
    Drug,
    LabValue,
    Procedure,
    VitalSign,
)

DEFAULT_PIVOT_CONFIG = {
    LabValue(): {
        'pivot_table_kwargs': {
            'columns': ['test_name'],
            'aggfunc': {'numeric_value': ['min', 'median', 'max']}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'numeric_value': ['min', 'median', 'max']}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'count'},
        },
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'count'},
        },
    },
    Diagnosis(): {
        'threshold': 0.1,
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'any'},
        }
    }
}

BINARY_PIVOT_CONFIG = {
    LabValue(): {
        'pivot_table_kwargs': {
            'columns': ['test_name'],
            'aggfunc': {'numeric_value': 'any'}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'numeric_value': 'any'}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'any'},
        },
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'any'},
        },
    },
    Diagnosis(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': 'any'},
        }
    }
}

COUNTED_PIVOT_CONFIG = {
    LabValue(): {
        'pivot_table_kwargs': {
            'columns': ['test_name'],
            'aggfunc': {'numeric_value': ['count']}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'numeric_value': ['count']}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': ['count']},
        }
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': ['count']},
        },
    },
    Diagnosis(): {
        'pivot_table_kwargs': {
            'columns': ['code'],
            'aggfunc': {'code': ['count']},
        }
    }
}
