The `username` and `password` fields do not need to be specified if you use `kerberos` and/or a VOMS proxy for authentication.

The optional `protocol` setting can be used to pass a preferred XRootD authentication protocol list to the client, for example `krb5` or `krb5,unix`. This is useful for setups where relying on external `XrdSecPROTOCOL` forwarding would be fragile.

The optional `url_decorator` argument can be used to pass a function to modify the root URL.

A possible use-case would be a function that wraps the URL with a token to allow for authentication.

If both `protocol` and `url_decorator` are used, the plugin adds the `xrd.wantprot` query parameter first and then applies the decorator. Decorators therefore need to handle URLs that may already contain query parameters.
