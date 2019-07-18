from fiber.condition import (
    Procedure,
    Diagnosis,
    Patient,
    VitalSign,
    Material,
    LabValue,
)

'''
from fiber.cohort import Cohort
from fiber.database.table import fact
min_age = fact.AGE_IN_DAYS > 365 * 18

hs = (
    Procedure(code='35.%').with_(min_age) |
    Procedure(code='36.1%').with_(min_age)
)
ps = (
    Patient(gender='Male') &
    Patient(religion='Catholic') &
    Patient(race='Hispanic/Latino')
)
aki = Diagnosis(code='584.9', context='ICD-9')

heart_surgery_cohort = Cohort(hs, limit=10).get(Procedure('35.%'), limit=10)
patient_cohort = Cohort(ps, limit=10).get(VitalSign('Pulse Apical'), limit=10)
print(patient_cohort)
print(heart_surgery_cohort)
'''

print(Procedure(code='30.%').example_values().to_string())
print(Diagnosis(code='%10%').example_values().to_string())
print(VitalSign('%pulse%').example_values().to_string())
print(Material('%heart%').example_values().to_string())
print(Patient(gender='Male').example_values().to_string())
print(LabValue(test_name='%SERUM%').example_values().to_string())

# print(len(patient_cohort))
