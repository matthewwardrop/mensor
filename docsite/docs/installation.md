If your company/organisation has provided a package that wraps around `mensor`
to provide a library of measure and metric providers, then a direct installation
of `mensor` is not required. Otherwise, you can install it using the standard
Python package manager: `pip`. If you use Python 3, you may need to change `pip`
references to `pip3`, depending on your system configuration.

```
pip install mensor
```

Note that this only installs the mensor computation engine, and that you will
need to construct your own library of measures and metrics if you use it
directly. To get started with this, please review the :doc:`quickstart`.
