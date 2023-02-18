Concepts
========

Although Mensor is designed to be intuitive, the nature of the work it performs
(metric and measure computation) requires precision and accuracy. As such, it is
crucial that users of Mensor know *exactly* how it works, and what assumptions
are made in every operation. This resource will cover the core concepts behind
mensor. In the :doc:`quickstart` documentation concrete examples are provided.

Terminology
-----------

Mensor uses somewhat standard terminology (derived from statistics and star
database schemas), but for clarity we spell out exactly what is meant by these
terms in the Mensor universe.

Statistical Unit:
    The indivisible unit of an analysis, which acts as a single sample in
    statistical analysis. Examples include: a user, a country, a session, or a
    document. In this documentation and throughout Mensor, this is used
    interchangeable with "Statistical Unit Type", "Unit Type" or "Identifier".

Dimension:
    A feature of a statistical unit which can be used to segment statistical
    units into groups. Examples include: country of a user, number of hours
    spent in a session, etc.

Measure:
    An extensive feature of a statistical unit that can be aggregated across
    other statistical units of the same type. Note that measures are a subset of
    dimensions. Examples include: number of hours spent in a session, number of
    pets owned by users, etc.

Metric:
    An arbitrary function of measures. Metrics cannot be further aggregated.
    Examples include: the average number of hours spent in a sessions per user,
    the average population per country, etc.

Measure Provider:
    A Python object capable of providing data for a collection of unit types,
    dimensions, and measures.

Measure Registry:
    A Python object into which an arbitrary number of measure providers are
    registered that creates a graph of relationships between providers, unit
    types and related dimensions and measures, that can then intelligently
    extract data for any given unit type from all relevant data sources,
    performing any required joins automatically.

Join:
    A merging of data associated with a statistical unit from two measure
    providers.

Partition:
    A dimension which logically segments data from a measure provider(s) into
    chunks that can be meaningfully joined. Examples include: the date of the
    data, which should be used in joins to ensure, for example, that data from
    a "fact" table in star schema is only ever joined with data from the same
    date in a corresponding "dimension" table.

Constraint:
    A condition that must be satisfied for data to included in the result-set
    of an evaluation.

Evaluation:
    A computation to generate data associated with a nominated unit type for
    nominated measures, segmented by nominated dimensions, and subject to
    nominated constraints.

The Architecture of Mensor
--------------------------

TBD.

The Grammar of Measures and Metrics
-----------------------------------

As it happens, the set of things that one typically wants to do with data in
order to generate measures and metrics from data sources is sufficiently
restrictive that you can write a grammar for it that is both intuitive and
powerful. In this section we explore the key tenets of this grammar.

**1) All analyses assume a statistical unit type**

While often implicit, it is always the case that for a measure/metric to be
meaningful that it must be associated with a particular unit type. Mensor makes
this choice of unit type explicit, which allows it to automatically compute
relevant statistics (including variance, etc).

**2) Joins are always implicit from context**

In Mensor, measure providers provide all necessary information to uniquely
determine the optimal joins to perform from context. As a result, Mensor never
requires the user to perform explicit joins between data from different measure
providers. Instead, joins are implicitly performed whenever required. Examples
of this are provided in the :doc:`quickstart` section.

**3) Unit types can be hierarchical**

It is often the case that a unit type can be considered in some sense a
subclass of another unit type; for example, users who are also sellers. The
grammar adopted by Mensor allows features of more general types to be transitive
through to more specific types.

The specifics of this grammar will be explored in more detail in the
:doc:`quickstart`.
