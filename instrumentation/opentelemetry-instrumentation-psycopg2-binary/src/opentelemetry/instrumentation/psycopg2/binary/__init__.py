# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The integration with PostgreSQL supports the `Psycopg`_ library, it can be enabled by
using ``Psycopg2Instrumentor``.

.. _Psycopg: http://initd.org/psycopg/

SQLCOMMENTER
*****************************************
You can optionally configure Psycopg2 instrumentation to enable sqlcommenter which enriches
the query with contextual information.

Usage
-----

.. code:: python

    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor

    Psycopg2Instrumentor().instrument(enable_commenter=True, commenter_options={})


For example,
::

   Invoking cursor.execute("select * from auth_users") will lead to sql query "select * from auth_users" but when SQLCommenter is enabled
   the query will get appended with some configurable tags like "select * from auth_users /*tag=value*/;"


SQLCommenter Configurations
***************************
We can configure the tags to be appended to the sqlquery log by adding configuration inside commenter_options(default:{}) keyword

db_driver = True(Default) or False

For example,
::
Enabling this flag will add psycopg2 and it's version which is /*psycopg2%%3A2.9.3*/

dbapi_threadsafety = True(Default) or False

For example,
::
Enabling this flag will add threadsafety /*dbapi_threadsafety=2*/

dbapi_level = True(Default) or False

For example,
::
Enabling this flag will add dbapi_level /*dbapi_level='2.0'*/

libpq_version = True(Default) or False

For example,
::
Enabling this flag will add libpq_version /*libpq_version=140001*/

driver_paramstyle = True(Default) or False

For example,
::
Enabling this flag will add driver_paramstyle /*driver_paramstyle='pyformat'*/

opentelemetry_values = True(Default) or False

For example,
::
Enabling this flag will add traceparent values /*traceparent='00-03afa25236b8cd948fa853d67038ac79-405ff022e8247c46-01'*/

Usage
-----

.. code-block:: python

    import psycopg2
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor


    Psycopg2Instrumentor().instrument()

    cnx = psycopg2.connect(database='Database')
    cursor = cnx.cursor()
    cursor.execute("INSERT INTO test (testField) VALUES (123)")
    cursor.close()
    cnx.close()

API
---
"""
from opentelemetry.instrumentation.psycopg2 import DatabaseApiIntegration, CursorTracer
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.psycopg2.binary.version import __version__

class Psycopg2BinaryInstrumentor(Psycopg2Instrumentor):
    pass

# TODO(owais): check if core dbapi can do this for all dbapi implementations e.g, pymysql and mysql
class DatabaseApiIntegration(DatabaseApiIntegration):
    pass


class CursorTracer(CursorTracer):
    pass
