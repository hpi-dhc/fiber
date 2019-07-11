def calc_e_gfr(creatinine_value, age, gender, race):
    """
    Formula based on the CKD-EPI Creatinine Equation (2009) as recommended by
    the US National Kidney Foundation.
    The original paper can be found at
    https://www.ncbi.nlm.nih.gov/pubmed/19414839.
    This equation is only valid for adults.
    """
    is_female = gender in ["Female"]
    is_black = race in [
        "Black Or African-American",
        "African American (Black)"
    ]
    alpha = -0.329 if is_female else -0.411
    k = 0.7 if is_female else 0.9
    return (
        141
        * min(creatinine_value / k, 1)**alpha
        * max(creatinine_value / k, 1)**-1.209
        * 0.993**age
        * (1.018 if is_female else 1)
        * (1.159 if is_black else 1)
    )
