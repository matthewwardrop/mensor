"""
Mensor is a graph-based computation engine for computing measures and metrics.
It:

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
outside of Mensor itself will resulting a major version bump. If you pin the
version of Mensor used in your project using `mensor>=x.y(.z)?<x.y+1`
(e.g. `mensor>=0.1.2<0.2`), you should be protected from any code churn and can
upgrade to newer versions of Mensor after reading the release notes at your
leisure.
"""

from setuptools import find_packages, setup

version_info = {}
with open('mensor/_version.py') as version_file:
    exec(version_file.read(), version_info)

setup(
    name='mensor',
    description="A dynamic graph-based metric computation engine.",
    long_description=__doc__.strip(),
    long_description_content_type='text/markdown',
    version=version_info['__version__'],
    author=version_info['__author__'],
    author_email=version_info['__author_email__'],
    license='MIT',
    url='http://github.com/airbnb/mensor',
    project_urls={
        'Documentation': 'http://mensor.readthedocs.io',
        'Source': 'https://github.com/airbnb/mensor',
        'Issue Tracker': 'https://github.com/airbnb/mensor/issues',
    },
    keywords='measures metrics aggregation experimentation statistics',
    packages=find_packages(),
    python_requires='~=3.4',
    install_requires=version_info['__dependencies__'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
