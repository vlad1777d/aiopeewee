import aiosqlite

from peewee import sqlite3, ImproperlyConfigured, basestring
from peewee import sort_models_topologically
from peewee import (SqliteDatabase, IndexMetadata,
                    ColumnMetadata, ForeignKeyMetadata)

from .database import AioDatabase, AioConnection


class AioSqliteDatabase(AioDatabase, SqliteDatabase):

    async def connect(self, database, **kwargs):
        #self.database = database
        #self.connection = aiosqlite.connect(database)
        #self.connection = await self.connection.__aenter__()
        #self.connection.execute_sql = self.connection.execute  # because of compatibility
        # move somewhere to init
        # make this method dummy
        #return self.connection
        pass

    async def close(self):
        #await self.connection.__aexit__(None, None, None)
        pass

    async def create_tables(self, models, fail_silently=False):
        """Create tables for all given models (in the right order)."""
        for model in sort_models_topologically(models):
            await model.create_table(fail_silently)

    async def create_table(self, model_class, safe=False):
        qc = self.compiler()  # from peewee.Database
        args = qc.create_table(model_class, safe)
        async with self.get_conn() as connection:
            result = await connection.execute(*args)
        return result

    async def get_tables(self, schema=None):
        async with self.get_conn() as connection:
            cursor = await connection.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row for row, in await cursor.fetchall()]
        return tables

    async def create_index(self, model_class, fields, unique=False):
        if not isinstance(fields, (list, tuple)):
            raise ValueError(f'Fields passed to "create_index" must be a list or tuple: {fields}')

        qc = self.compiler()
        fobjs = [model_class._meta.fields[f]
                    if isinstance(f, basestring) else f
                    for f in fields]
        args = qc.create_index(model_class, fobjs, unique)

        async with self.get_conn() as connection:
            result = await connection.execute(*args)
        return result

    async def get_indexes(self, table, schema=None):
        unique = set()
        indexes = {}
        sql = f'SHOW INDEX FROM {table}'
        async with self.get_conn() as connection:
            cursor = await connection.execute(sql)

            for row in cursor.fetchall():
                if not row[1]:
                    unique.add(row[2])
                indexes.setdefault(row[2], [])
                indexes[row[2]].append(row[4])

        return [IndexMetadata(name, None, indexes[name], name in unique, table)
                for name in indexes]

    async def get_columns(self, table, schema=None):
        sql = """
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = DATABASE()"""

        async with self.get_conn() as connection:
            cursor = await connection.execute(sql, (table,))
            rows = await cursor.fetchall()
            pks = set(self.get_primary_keys(table))

        return [ColumnMetadata(name, dt, null == 'YES', name in pks, table)
                for name, null, dt in rows]

    async def get_primary_keys(self, table, schema=None):

        sql = 'SHOW INDEX FROM {table}'
        async with self.get_conn() as connection:
            cursor = await connection.execute(sql)
            rows = await cursor.fetchall()
        return [row[4] for row in rows if row[2] == 'PRIMARY']

    async def get_foreign_keys(self, table, schema=None):
        query = """
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL"""

        async with self.get_conn() as connection:
            cursor = await connection.execute(query, (table,))
            rows = await cursor.fetchall()
        return [ForeignKeyMetadata(column, dest_table, dest_column, table)
                for column, dest_table, dest_column in rows]

    def get_binary_type(self):
        return sqlite3.Binary

    '''def get_conn(self):
        """For compatibility with other aiopeewee modules
            (like 'query')
            which use it's result in 'async with' statement.
        """
        print('GETTING CON')
        def empty__aenter__(self):
            """Used for compatibility of 'self.connection' with 'async with'
            blocks.
            """
            return self
        def empty__aexit__(type, value, traceback):
            return False

        self.connection.__aenter__ = empty__aenter__
        self.connection.__aexit__ = empty__aexit__
        self.connection.execute_sql = self.connection.execute
        # (aiomysql wrapper uses such method, other aiopeewee modules expect it)
        return self.connection'''

    def get_conn(self):
        connection = aiosqlite.connect(self.database)
        #connection.execute_sql = self.connection.execute
        #return connection

        aioconnection = AioConnection(connection,
                             autocommit=self.autocommit,
                             autorollback=self.autorollback,
                             exception_wrapper=self.exception_wrapper)
        aioconnection.execute = aioconnection.execute_sql

        return aioconnection

    def set_connection(self):
        """Creates global connection.
        Is used FROM such 
        """
        connection = aiosqlite.connect(self.database)
        connection.execute_sql = self.connection.execute
        self.connection = await self.connection.__aenter__()
        return connection

    def close_connection(self):
        await self.connection.__aexit__(None, None, None)
