# Mensor
[![PyPI Version](https://img.shields.io/pypi/v/mensor.svg)](https://pypi.org/project/mensor/)
![Project Status](https://img.shields.io/pypi/status/mensor.svg)
![Python Versions](https://img.shields.io/pypi/pyversions/mensor.svg)
[![Build Status](https://img.shields.io/github/actions/workflow/status/matthewwardrop/mensor/tests.yml?branch=main)](https://github.com/matthewwardrop/mensor/actions?query=workflow%3A%22Run+Tests%22)
[![Documentation Status](https://readthedocs.org/projects/mensor/badge/?version=latest)](http://mensor.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/github/license/matthewwardrop/mensor.svg)](https://github.com/matthewwardrop/mensor/blob/master/LICENSE)

Mensor is a graph-based computation engine for computing measures and metrics.

- **Documentation:** http://mensor.readthedocs.io
- **Source:** https://github.com/matthewwardrop/mensor
- **Bug reports:** https://github.com/matthewwardrop/mensor/issues

Among other things, Mensor:

- defines a new grammar for extracting measures and metrics that is designed to
  be intuitive and capable (it can do almost(?) anything that makes sense to
  do with metrics and measures).
- makes measure and metric definitions explicit and shareable, and their
  computations transparent and reproducible.
- allows multiple data sources to be stitched together on the fly without users
  having to explicitly write the code / query required to join the data sources.
- is agnostic as to how data is stored or accessed, and new data backends are
  relatively simple to write.
- allows for local ad-hoc definitions of additional data sources for exploration
  by data scientists or other technically minded folk, decoupling it from
  deployment into production services.

**Note:** Mensor is currently still under heavy development, and intrusive
changes to the API are expected. To minimise the impact on downstream projects,
Mensor will strictly adhere to semantic versioning. In particular, any
incompatible or sufficiently adventurous change to classes expected to be used
outside of Mensor itself will result in a minor version bump. If you pin the
version of Mensor used in your project using `mensor>=x.y(.z)?<x.y+1`
(e.g. `mensor>=0.1.2<0.2`), you should be protected from any code churn and can
upgrade to newer versions of Mensor after reading the release notes at your
leisure.
