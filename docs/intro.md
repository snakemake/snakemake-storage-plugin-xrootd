A Snakemake storage plugin to read and write via the [XRootD protocol](https://xrootd.slac.stanford.edu/).

Currently, only files can be used as inputs or outputs and not directories.

The plugin can be used without specifying any options relating to the URLs, in which case all information must be contained in the URL passed by the user.

The options for `host`, `port`, `username`, `password` and `url_decorator` can be specifed to make the URLs shorter and easier to use.

Please note: if the `password` option is supplied (even implicitly via the environment variable `SNAKEMAKE_STORAGE_XROOTD_PASSWORD`) it will be displayed in plaintext as part of the XRootD URLs when Snakemake prints information about a rule. Only use the `password` option in trusted environments.
