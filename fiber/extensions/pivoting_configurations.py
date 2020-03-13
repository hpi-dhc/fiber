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
            'columns': ['description'],
            'aggfunc': {'numeric_value': ['min', 'median', 'max']}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'numeric_value': ['min', 'median', 'max']}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'count'},
        },
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'count'},
        },
    },
    Diagnosis(): {
        'threshold': 0.1,
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'any'},
        }
    }
}

BINARY_PIVOT_CONFIG = {
    LabValue(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'numeric_value': 'any'}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'numeric_value': 'any'}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'any'},
        },
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'any'},
        },
    },
    Diagnosis(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': 'any'},
        }
    }
}

COUNTED_PIVOT_CONFIG = {
    LabValue(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'numeric_value': ['count']}
        },
    },
    VitalSign(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'numeric_value': ['count']}
        },
    },
    Procedure(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': ['count']},
        }
    },
    Drug(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': ['count']},
        },
    },
    Diagnosis(): {
        'pivot_table_kwargs': {
            'columns': ['description'],
            'aggfunc': {'description': ['count']},
        }
    }
}
