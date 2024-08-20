The `username` and `password` fields do not need to be specified if you use `kerberos` and/or a VOMS proxy for authentication.

The optional `url_decorator` argument can be used to pass a function to modify the root URL.

A possible use-case would be a function that wraps the URL with a token to allow for authentication.
