Arctic Tern: Simple Postgres migrations for Python
==================================================
.. image:: https://travis-ci.org/ConvergysLabs/arctic-tern.svg?branch=master
    :target: https://travis-ci.org/ConvergysLabs/arctic-tern

.. image:: https://codecov.io/github/ConvergysLabs/arctic-tern/coverage.svg?branch=master
    :target: https://codecov.io/github/ConvergysLabs/arctic-tern
    :alt: codecov.io

.. image:: https://upload.wikimedia.org/wikipedia/commons/2/29/2009_07_02_-_Arctic_tern_on_Farne_Islands_-_The_blue_rope_demarcates_the_visitors%27_path.JPG
    :target: https://en.wikipedia.org/wiki/Arctic_tern

You can be strongly migratory, too!

Feature Support
---------------

- Plain SQL update scripts
- Timestamped update scripts
- Code-level integration (no CLI needed)
- Asyncio compatibiltity using asyncpg_

Arctic Tern officially supports Python 3.8+

Installation
------------

To install Arctic Tern, simply::

    $ pip install arctic-tern


How to Contribute
-----------------

Arctic Tern uses poetry_ for dependency management and packaging.
To install::

    $ poetry install

To install with asyncpg_::

    $ poetry install -E asyncpg

To install with psycopg_::

    $ poetry install -E psycopg

To build the distributable::

    $ poetry build

To publish the package to PyPI_::

    $ poetry publish


Documentation
-------------

Documentation is good!  We should get some.


.. _poetry: https://python-poetry.org/
.. _asyncpg: https://github.com/MagicStack/asyncpg
.. _psycopg: https://www.psycopg.org/
.. _PyPI: https://pypi.org/project/arctic-tern/