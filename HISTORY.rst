0.1.0 (2014-10-31)
++++++++++++++++++

* Initial release

0.1.1 (2014-11-12)
++++++++++++++++++

* Fixing ``pip`` installation problem

0.2.0 (2014-12-05)
++++++++++++++++++

* Force loading of all descendents of forced rows

0.2.1 (2014-12-28)
++++++++++++++++++

* Accept ``schema`` argument
* Handle foreign keys that cross schema boundaries

0.2.2 (2015-02-06)
++++++++++++++++++

* Set sequence values in test DB to avoid unique constraint violations (PG only)

0.2.3 (2015-05-24)
++++++++++++++++++

* Fixed ``input``/``raw_input`` Py2/3 bug (thanks mrchrisadams)
* Let `python subsetter.py` run directly, without installation (thanks amitsaha)
* Manually-specified constraints (thanks jmcarp)
* `--exclude-table` argument (jmcarp)
* Optimize by using bulk inserts (jmcarp)

0.2.4 (2015-07-08)
++++++++++++++++++

* wildcards for `--exclude-table` (thanks jmcarp)
* `--full-table` arg

0.2.5 (2015-11-16)
++++++++++++++++++

* `--table` argument (birdonfire)
* table-related arguments support schema prefixes (birdonfire)
* Sequence updating now respects `--table` and `--exclude-table` (birdonfire)
* Force float division in ``_completeness_score`` (birdonfire)
* Add timestamp to log output (birdonfire)
* Fetch parent rows required by configured constraints (birdonfire)
* Respect cross-schema constraints (birdonfire)
* Support qualified table names in constraint keys (birdonfire)
* guarantee fetching 100% of `--full-table` tables (birdonfire)
* allow disabling of write buffering (brki)
* support event handlers (brki)
* packaging changed to wheel (jmcarp)
* documentation bug cleanup (jmcarp)
* use `coverage` (jmcarp)

0.2.6 (2017-10-17)
++++++++++++++++++

* Correct random row choice for MSSQL (Joe Francis)
* Adjust sequences only for specified schemas (R Vrajmohan)
* Fix for arrays of ENUM (Federico Bertola)
* Allow tables and schemas in config file (Alex Codreanu)

0.2.6.1 (2017-10-17)
++++++++++++++++++++

* Fixed bug in packaging

0.2.6.2 (2017-10-17)
++++++++++++++++++++

* Bugfix in array-of-enum handling