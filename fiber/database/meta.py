# flake8: noqa
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData

def add_tables(meta):

    D_PERSON = Table(
        'D_PERSON', meta,
        Column('PERSON_KEY', Integer, primary_key=True, default=0),
        Column('PERSON_TYPE', String, default=''),
        Column('MEDICAL_RECORD_NUMBER', String, default=''),
        Column('MOTHER_ACCOUNT_NUMBER', String, default=''),
        Column('DATE_OF_BIRTH', DateTime, default=''),
        Column('GENDER', String, default=''),
        Column('RACE', String, default=''),
        Column('CITIZENSHIP', String, default=''),
        Column('LANGUAGE', String, default=''),
        Column('MARITAL_STATUS_CODE', String, default=''),
        Column('MARITAL_STATUS_LEGAL_IND', String, default=''),
        Column('MARITAL_STATUS_SOCIAL_IND', String, default=''),
        Column('MARITAL_STATUS_REASON', String, default=''),
        Column('RELIGION', String, default=''),
        Column('ADDRESS_TYPE', String, default=''),
        Column('ADDRESS_ZIP', String, default=''),
        Column('ADDRESS_ID', String, default=''),
        Column('DECEASED_INDICATOR', String, default=''),
        Column('LIVING_WILL_INDICATOR', String, default=''),
        Column('ACTIVE_INDICATOR', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
        Column('VALID_FLAG', String, default='Y'),
        Column('SOURCE_NAME', String, default=''),
        Column('PERSON_CONTROL_KEY', Integer, default=0),
        Column('PATIENT_ETHNIC_GROUP', String, default=''),
        Column('MONTH_OF_BIRTH', String, default=''),
    )

    FACT = Table(
        'FACT', meta,
        Column('FACT_KEY', Integer, primary_key=True, default=0),
        Column('PERSON_KEY', Integer, default=0),
        Column('ACCOUNTING_GROUP_KEY', Integer, default=0),
        Column('ENCOUNTER_KEY', Integer, default=0),
        Column('CAREGIVER_GROUP_KEY', Integer, default=0),
        Column('FACILITY_KEY', Integer, default=0),
        Column('PROCEDURE_GROUP_KEY', Integer, default=0),
        Column('DIAGNOSIS_GROUP_KEY', Integer, default=0),
        Column('MATERIAL_GROUP_KEY', Integer, default=0),
        Column('ORGANIZATION_GROUP_KEY', Integer, default=0),
        Column('PAYOR_GROUP_KEY', Integer, default=0),
        Column('TIME_OF_DAY_KEY', Integer, default=0),
        Column('META_DATA_KEY', Integer, default=0),
        Column('UOM_KEY', Integer, default=0),
        Column('DATA_STATE_KEY', Integer, default=0),
        Column('VALUE', String, default=''),
        Column('AGE_IN_DAYS', Integer, default=0),
    )

    FD_MATERIAL = Table(
        'FD_MATERIAL', meta,
        Column('MATERIAL_KEY', Integer, primary_key=True, default=0),
        Column('MATERIAL_TYPE', String, default=''),
        Column('MATERIAL_NAME', String, default=''),
        Column('GENERIC_NAME', String, default=''),
        Column('BRAND1', String, default=''),
        Column('BRAND2', String, default=''),
        Column('SOURCE_NAME', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
        Column('VALID_FLAG', String, default='Y'),
        Column('MATERIAL_CONTROL_KEY', Integer, default=0),
        Column('CONTEXT_MATERIAL_CODE', String, default=''),
        Column('CONTEXT_NAME', String, default=''),
    )

    B_MATERIAL = Table(
        'B_MATERIAL', meta,
        Column('ID', Integer, primary_key=True, default=0),
        Column('MATERIAL_KEY', Integer, default=0),
        Column('MATERIAL_GROUP_KEY', Integer, default=0),
        Column('MATERIAL_ROLE', String, default=''),
        Column('MATERIAL_RANK', Integer, default=0),
        Column('MATERIAL_WEIGHT_FACTOR', Integer, default=0),
    )

    FD_DIAGNOSIS = Table(
        'FD_DIAGNOSIS', meta,
        Column('DIAGNOSIS_KEY', Integer, primary_key=True, default=0),
        Column('DIAGNOSIS_TYPE', String, default=''),
        Column('DESCRIPTION', String, default=''),
        Column('SOURCE_NAME', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
        Column('VALID_FLAG', String, default='Y'),
        Column('DIAGNOSIS_CONTROL_KEY', Integer, default=0),
        Column('CONTEXT_DIAGNOSIS_CODE', String, default=''),
        Column('CONTEXT_NAME', String, default=''),
    )

    B_DIAGNOSIS = Table(
        'B_DIAGNOSIS', meta,
        Column('ID', Integer, default=0),
        Column('DIAGNOSIS_KEY', Integer, default=0),
        Column('DIAGNOSIS_GROUP_KEY', Integer, default=0),
        Column('DIAGNOSIS_ROLE', String, default=''),
        Column('DIAGNOSIS_RANK', Integer, default=0),
        Column('DIAGNOSIS_WEIGHT_FACTOR', Integer, default=0),
    )

    FD_PROCEDURE = Table(
        'FD_PROCEDURE', meta,
        Column('ID', Integer, primary_key=True, default=0),
        Column('PROCEDURE_KEY', Integer, default=0),
        Column('PROCEDURE_TYPE', String, default=''),
        Column('PROCEDURE_DESCRIPTION', String, default=''),
        Column('SOURCE_NAME', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
        Column('VALID_FLAG', String, default='Y'),
        Column('PROCEDURE_CONTROL_KEY', Integer, default=0),
        Column('CONTEXT_PROCEDURE_CODE', String, default=''),
        Column('CONTEXT_NAME', String, default=''),
    )

    B_PROCEDURE = Table(
        'B_PROCEDURE', meta,
        Column('ID', Integer, primary_key=True, default=0),
        Column('PROCEDURE_KEY', Integer, default=0),
        Column('PROCEDURE_GROUP_KEY', Integer, default=0),
        Column('PROCEDURE_ROLE', String, default=''),
        Column('PROCEDURE_RANK', Integer, default=0),
        Column('PROCEDURE_WEIGHT_FACTOR', Integer, default=0),
    )

    D_UNIT_OF_MEASURE = Table(
        'D_UNIT_OF_MEASURE', meta,
        Column('UOM_KEY', Integer, primary_key=True, default=0),
        Column('UOM_CLASS', String, default=''),
        Column('UNIT_OF_MEASURE', String, default=''),
        Column('SOURCE_NAME', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
    )

    D_ENCOUNTER = Table(
        'D_ENCOUNTER', meta,
        Column('ENCOUNTER_KEY', Integer, primary_key=True, default=0),
        Column('ENCOUNTER_TYPE', String, default=''),
        Column('ENCOUNTER_VISIT_ID', String, default=''),
        Column('ENCOUNTER_ACCOUNT_NUMBER', String, default=''),
        Column('MEDICAL_RECORD_NUMBER', String, default=''),
        Column('EVENT_TYPE', String, default=''),
        Column('ADMISSION_TYPE', String, default=''),
        Column('ENCOUNTER_CLASS', String, default=''),
        Column('ADMISSION_SOURCE', String, default=''),
        Column('ENCOUNTER_ACCOUNT_STATUS', String, default=''),
        Column('ENCOUNTER_SERVICE', String, default=''),
        Column('ENCOUNTER_ACCOMODATION', String, default=''),
        Column('SPECIALTY_UNIT', String, default=''),
        Column('DISCHARGE_DISPOSITION', String, default=''),
        Column('DISCHARGE_LOCATION_TO', String, default=''),
        Column('ESTIMATED_LENGTH_OF_STAY', Integer, default=0),
        Column('BEGIN_DATE_AGE_IN_DAYS', Integer, default=0),
        Column('END_DATE_AGE_IN_DAYS', Integer, default=0),
        Column('SOURCE_NAME', String, default=''),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
        Column('VALID_FLAG', String, default='Y'),
        Column('ENCOUNTER_CONTROL_KEY', Integer, default=0),
        Column('VISIT_OWNER', String, default=''),
        Column('ENCOUNTER_SUB_VISIT_ID', String, default=''),
        Column('EPIC_ENCOUNTER_TYPE', String, default=''),
        Column('MSDW_ENCOUNTER_TYPE', String, default=''),
        Column('EPIC_ENCOUNTER_STATUS', String, default=''),
    )

    EPIC_LAB = Table(
        'EPIC_LAB', meta,
        Column('ID', Integer, primary_key=True, default=0),
        Column('ORDER_ID', String, default=''),
        Column('ORDER_LINE', Integer, default=0),
        Column('MEDICAL_RECORD_NUMBER', String, default=''),
        Column('AGE_IN_DAYS', Integer, default=0),
        Column('TEST_CODE', Integer, default=0),
        Column('TEST_NAME', String, default=''),
        Column('LAB_STATUS', String, default=''),
        Column('RESULT_STATUS', String, default=''),
        Column('RESULT_FLAG', String, default=''),
        Column('ABNORMAL_FLAG', String, default=''),
        Column('REFERENCE_RANGE', String, default=''),
        Column('UNIT_OF_MEASUREMENT', String, default=''),
        Column('TEST_RESULT_VALUE', String, default=''),
    )

    D_METADATA = Table(
        'D_METADATA', meta,
        Column('META_DATA_KEY', Integer, primary_key=True, default=0),
        Column('CONTEXT_KEY', Integer, default=0),
        Column('EXPECTED_UOM_KEY', Integer, default=0),
        Column('LEVEL1_CONTEXT_NAME', String, default=''),
        Column('LEVEL2_EVENT_NAME', String, default=''),
        Column('LEVEL3_ACTION_NAME', String, default=''),
        Column('LEVEL4_FIELD_NAME', String, default=''),
        Column('HIPAA_FLAG', String, default='N'),
        Column('ACTIVE_FLAG', String, default='Y'),
        Column('ORPHAN_FLAG', String, default='N'),
    )

    return meta
