from fiber.cohort import Cohort
from fiber.condition import Procedure, Diagnosis, Patient
from fiber.database.table import fact
min_age = fact.AGE_IN_DAYS > 365 * 18

hs = (
    Procedure(code='35.%').with_(min_age) |
    Procedure(code='36.1%').with_(min_age)
)
ps = (
    Patient(gender='Male') &
    Patient(religion='Catholic')
)
aki = Diagnosis(code='584.9', context='ICD-9')
heart_surgery_cohort = Cohort(hs)
patient_cohort = Cohort(ps)

print(len(heart_surgery_cohort))
print(len(heart_surgery_cohort.exclude(['2241492414', '2117141414']).mrns()))

print(len(patient_cohort))
