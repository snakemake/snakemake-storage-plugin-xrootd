# decorator must be a string with an importable function so put one here
def add_decorator(url: str) -> str:
    return url + "?authz=anonymous"


def add_query_aware_decorator(url: str) -> str:
    marker = "saw_query=yes" if "?" in url else "saw_query=no"
    sep = "&" if "?" in url else "?"
    return url + f"{sep}{marker}"
