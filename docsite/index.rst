=================================
Mensor |release| documentation
=================================

.. toctree::
    :hidden:

    concepts
    installation
    quickstart
    deployment
    contributions

Welcome! If this is the first time that you have stumbled across this
documentation, there is a very good chance you have some questions about this
project. That's fantastic! Hopefully, these resources will go some way toward
answering them. If you find it lacking in any way, please do not hesitate to
file an issue on the `GitHub issue tracker <http://github.com/matthewwardrop/mensor/issues>`_.

What is Mensor?
---------------

Mensor is a graph-based computation engine for computing measures and metrics. It:

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

Why does Mensor exist?
----------------------

In short, the author (Matthew Wardrop) became frustrated with some (perceived?)
operational inefficiencies endemic to the data science industry. In particular,
he observed that substantial portions of data science work hours were spent
reproducing statistics shown in dashboards, defining ad-hoc segmentations in
SQL, and then endlessly debugging them. To make matters worse, despite these
efforts taking a significant amount of time, there was little persistence of
their efforts beyond their particular analyses, meaning that very similar
analyses being done on opposite sides of the company (or done a few months
later) all started from scratch. Mensor was created to solve these problems.

How do I use Mensor?
--------------------

I like where you are going with this line of inquiry! If you are new to Mensor,
check out the :doc:`concepts` documentation, and then proceed with the
:doc:`installation` instructions. Once installed, you can kickstart your efforts
using the :doc:`quickstart` documentation. If you are looking to deploy Mensor
as part of a Python package for a team or for production environments, consider
exploring the :doc:`deployment` material.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
