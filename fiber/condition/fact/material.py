from fiber.condition.database import _multi_like_clause
from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
)


class Material(_FactCondition):
    """
    The Material adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'MATERIAL' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, CONTEXT_NAME and the code_column in this case respectively.
    """
    dimensions = {'MATERIAL'}
    d_table = fd_mat
    code_column = fd_mat.CONTEXT_MATERIAL_CODE
    category_column = fd_mat.MATERIAL_TYPE
    description_column = fd_mat.MATERIAL_NAME

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]


class Drug(Material):
    """
    The Drug adds functionality to the Material. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'MATERIAL' and category 'Drug' (optionally age-constraints on the dates).

    Default-columns to return, MEDICAL_RECORD_NUMBER, AGE_IN_DAYS, CONTEXT_NAME
    and the code_column are defined in the super-class Material.
    """

    def __init__(
        self,
        name: str = '',
        brand: str = '',
        generic: str = '',
        *args,
        **kwargs
    ):
        """
        Args:
            name: the name of the drug to query for, referring to
                MATERIAL_NAME, GENERIC_NAME or BRAND_NAME
            args: the arguments to pass higher in the hierarchy
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self._attrs['name'] = name
        self._attrs['brand'] = brand
        self._attrs['generic'] = generic

    @property
    def name(self):
        """This shall return the drugs name, being either MATERIAL_NAME,
        GENERIC_NAME or BRAND_NAME."""
        return self._attrs['name']

    def _create_clause(self):
        clause = super()._create_clause()
        """
        Used to create a SQLAlchemy clause based on the Drug-condition.
        It is used to select the correct drugs based on the name provided at
        initialization-time.
        """

        if self.name:
            clause &= (
                _multi_like_clause(fd_mat.MATERIAL_NAME, self.name) |
                _multi_like_clause(fd_mat.GENERIC_NAME, self.name) |
                _multi_like_clause(fd_mat.BRAND1, self.name) |
                _multi_like_clause(fd_mat.BRAND2, self.name)
            )
        if self._attrs['brand']:
            clause &= (
                _multi_like_clause(fd_mat.BRAND1, self._attrs['brand']) |
                _multi_like_clause(fd_mat.BRAND2, self._attrs['brand'])
            )
        if self._attrs['generic']:
            clause &= _multi_like_clause(
                fd_mat.GENERIC_NAME, self._attrs['generic'])

        return clause
