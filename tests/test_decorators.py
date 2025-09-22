# decorator must be a string with an importable function so put one here
def add_decorator(url: str) -> str:
    return url + "?authz=anonymous"
