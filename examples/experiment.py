'''
from fiber.condition import Patient
from sqlalchemy.ext.serializer import loads, dumps
from fiber.database.hana import get_meta
pkl = dumps(Patient(gender='Male').clause)
print(loads(pkl, metadata=get_meta()))
print(Patient(gender='Male').clause.right.value)
'''
from fiber.condition import Procedure, Patient
from fiber.cohort import Cohort

from fiber.database.table import fact


min_age = fact.AGE_IN_DAYS > 365 * 18
hs = (
    Procedure(code='35.%').with_(min_age) |
    Procedure(code='36.1%').with_(min_age)
)

print(len(hs))
print(len(hs))
Cohort(hs).get(Patient())
Cohort(hs).get(Patient())
