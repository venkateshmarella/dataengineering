import os

def identify_even_or_odd(num,test_run=False):
    if not isinstance(num,int) and not isinstance(num,str):
        print(f"not a exact integer, converting to Integer now from {num}")
        num=int(num)
    elif isinstance(num,str):
        try:
            numf=float(num)
            num=int(numf)
        except Exception:
            return f"not a valid value for even or odd validations. given input value - {num}"
    
    if num % 2  == 0:
        return "Even"
    else:
        return "Odd"