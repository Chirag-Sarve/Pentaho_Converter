"""Pentaho → PySpark migration package.

- ``transformations`` — one module per .ktr (``run(spark, config)``)
- ``jobs`` — one workflow per .kjb (``run(spark, config)``)
- ``job_engine`` — success / failure / unconditional hop evaluator
"""
