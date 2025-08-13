System access control properties
=================================

.. contents::
    :local:
    :depth: 1

General properties
------------------

``access-control.name``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :doc:`string </language/types>`
* **Default value:** ``default``

The name of the access control implementation.

``access-control.config-files``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :doc:`string </language/types>`
* **Default value:** ``etc/access-control.properties``

Comma-separated list of access control configuration files. These files are
read in order, and later files override earlier files if they contain the same
property.

System tables optimization
--------------------------

``system.jdbc.schema-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :doc:`integer </language/types>`
* **Default value:** ``5``
* **Minimum value:** ``1``

The number of schemas to process in each batch when listing tables for 
``system.jdbc.tables`` queries. This property helps reduce the load on 
access control systems like OPA by batching schema processing instead of 
making individual access control requests for each schema.

When a query requests tables from all schemas (no schema filter), Trino will:

1. Group schemas into batches of the configured size
2. For each batch, collect all tables from those schemas
3. Make a single access control request per batch instead of per schema

This significantly reduces the number of requests to external access control 
systems, especially in environments with many schemas.

**Example:**
- With 100 schemas and batch size 5: 20 access control requests instead of 100
- With 100 schemas and batch size 10: 10 access control requests instead of 100

Note: When querying specific schemas (e.g., ``SHOW TABLES FROM schema1``), 
pagination is bypassed for optimal performance.

.. code-block:: properties

    system.jdbc.schema-batch-size=10