"""Abstract class for objects backed by MySQL tables.

When interfacing with MySQL, this uses pure dictionaries. When interfacing with
the rest of the app, it is object-oriented, returning instances of the class to
represent data.

It attempts to have a similar interface as a datastore entity, including

* Class.create() to instaniate an object in memory only
* Class.get() to query db
* Class.get_by_id() to get one object from the db
* instance.put() to save to db
* instance.to_dict() to get a client-unsafe dictionary based on db properties
* instance.to_client_dict() to get a client-safe dict of db props

Because there is no equivalent of ndb here, there's also

* Class.put_multi() which accepts a list of objects of _one kind_
  (ndb.put_multi can take mixed kinds).
* e = Class.put_for_index(e, 'my-index') which attempts to update rows in place
  if the new data matches a non-uid unique index. Notice that it returns the
  entity, which may have a different uid than the one being set.
* Class.delete_multi() which accepts a list of objects or id strings of _one
  kind_ (ndb.delete_multi can take mixed kinds).

Also similar to datastore entities, there's some sugar around JSON values. Any
db properties listed in the class property `json_props` are stored in the db
as JSON strings but converted to parsed python values when dealing with
objects.
"""

from _mysql import escape
from collections import namedtuple, OrderedDict
from functools import wraps
from MySQLdb.converters import conversions
import datetime
import inspect
import json
import logging
import math
import numbers
import random
import string  # generate_uid

from .cursor import CursorResult, SqlCursor
from .datastore_model import DatastoreModel
import mysql_connection
import util

Field = namedtuple('Field', ['name', 'type', 'length', 'unsigned', 'null',
                             'default', 'on_update'])


# A text field can hold 64 kilobytes, and utf8 encoded strings occupy, at
# worst, 4 bytes per character, for a conservative estimate of 16,000
# characters total storage.
# https://stackoverflow.com/questions/6766781/maximum-length-for-mysql-type-text#6766854
JSON_TEXT_VALUE_MAX = 10**4  # for any given value in the dict
JSON_TEXT_DICT_MAX = 10**2  # len() of the dict (number of keys)
JSON_TEXT_MAX = 16000  # len(json.dumps(myJsonDict))


class JsonTextValueLengthError(ValueError):
    pass


class JsonTextDictLengthError(ValueError):
    pass


class JsonTextLengthError(ValueError):
    pass


def hooked_put(put_fn):
    """Decorator to call before_put() and after_put() for entities being saved.

    Used in multiple methods below:
    * put_multi()
    * put_for_index()
    * put()
    """
    @wraps(put_fn)
    def wrapped(*args, **kwargs):
        # Here we have exactly the args seen by the put_fn.

        # Some wrapped fns are class methods, others are instance methods.
        # Class method arguments always begin `klass`, `entities`, while
        # instance method arguments always begin `self`, which is an entity.
        # Normalize these to a list of entities, and whatever arguments occur
        # afteward.
        if inspect.isclass(args[0]):
            entity_or_entities = args[1]
            pos_args = args[2:]
        else:
            entity_or_entities = args[0]
            pos_args = args[1:]

        if isinstance(entity_or_entities, (list, tuple)):
            entities = entity_or_entities
        else:
            entities = [entity_or_entities]

        init_kwargs_index = {}

        for e in entities:
            init_kwargs_index[e.uid] = e._init_kwargs.copy()
            e.before_put(init_kwargs_index[e.uid], *pos_args, **kwargs)

        result = put_fn(*args, **kwargs)

        # What's `result`?
        #
        # * put() - None
        # * put_for_index() - updated entity
        # * put_multi() - affected rows

        # What about db-created values, like timestamps?
        #
        # * put() - `self` (arg[0]) has been updated
        # * put_for_index() - returned entity (`result`) is updated
        # * put_multi() - not yet accessible, would need to query

        if isinstance(result, SqlModel) and len(entities) == 1:
            # For the case of put_for_index, we want to act on the new entity
            # (returned by the put_fn), which will have updated timestamps and
            # likely a different uid.
            init_kwargs = init_kwargs_index[entities[0].uid]
            result.after_put(init_kwargs, *pos_args, **kwargs)
        else:
            # For other put cases.
            for e in entities:
                e.after_put(init_kwargs_index[e.uid], *pos_args, **kwargs)

        return result

    return wrapped

class SqlModel(object):
    table = None

    # "Constants" to represent SQL-language values in python. For example, it
    # is straightfoward to represent a SQL string, `'foo'` in python as
    # `'foo'`, but it is not straightforward to represent the SQL keyword
    # `CURRENT_TIMESTAMP` in python.
    sql_null = object()
    sql_current_timestamp = object()

    # Changes behavior of generate_uid() to use 16 random characters instead
    # of the default 8, because there could be a larger population of
    # MySQL rows than our datastore entities, and because their
    # ids are never namespaced by the parent user (e.g. a typical task id is:
    # Task_abcdefgh.User_abcdefgh).
    id_length = 16

    @classmethod
    def create(klass, id=None, **kwargs):
        # Start with generic defaults for this class.
        params = {}
        for f in klass.py_table_definition['fields']:
            # Fields which default to CURRENT_TIMESTAMP are never INSERTed
            # explicitly, so skip them.
            if f.default is klass.sql_current_timestamp:
                continue
            elif f.default in (None, klass.sql_null):
                # The difference between whether a field can be empty in the db
                # isn't relevant on creation, only on later INSERTion, so treat
                # them the same here.
                params[f.name] = None
            elif f.type in ('bool', 'boolean') and f.default is not None:
                # Defaults for boolean fields are described so they make sense
                # in a CREATE TABLE query, i.e. in their integer 0/1 form.
                # Convert to pythonic booleans.
                params[f.name] = True if f.default == 1 else False
            elif f.name in getattr(klass, 'json_props', []):
                # Defaults for JSON fields are described so they make sense in
                # a CREATE TABLE query, i.e. in their JSON string form. Parse
                # the default for the object form.
                params[f.name] = json.loads(f.default)
            else:
                # Otherwise the default is a python value and we just use it.
                params[f.name] = f.default

        # Mix in the specified kwargs.
        params.update(**kwargs)

        # Generate a uid.
        params['uid'] = klass.generate_uid(parent=None, identifier=id)
        params['short_uid'] = klass.convert_uid(params['uid'])

        return klass(**params)

    @classmethod
    def property_types(klass):
        """Get a map of property names to their expected types.

        Controls what can be queried in a query string and can be written to in
        a PUT. Designed to play nice with ApiHandler.get_params().
        """
        property_types = {}
        for f in klass.py_table_definition['fields']:
            typ = None
            if f.name in ['uid', 'short_uid', 'created', 'modified']:
                # These are read-only from the client's perspective, so they
                # have no updatable "type".
                continue
            elif f.name in getattr(klass, 'json_props', []):
                # needs to come before the varchar or text check
                typ = 'json'
            elif f.type in ('varchar', 'text'):
                typ = unicode
            elif f.type == 'bool':
                typ = bool
            elif f.type.endswith('int'):
                typ = int
            elif f.type == 'datetime':
                typ = 'datetime'
            elif f.type == 'date':
                typ = 'date'
            # No ComputedProperties; those aren't editable

            if typ:
                property_types[f.name] = typ

        return property_types

    @classmethod
    def get_by_id(klass, row_id_or_ids):
        """Assumes each table has column `uid` with unique index."""
        if row_id_or_ids is None:
            return None
        elif isinstance(row_id_or_ids, basestring):
            ids = set((row_id_or_ids,))
            return_single = True
        elif len(row_id_or_ids) == 0:
            return []
        else:
            ids = set(row_id_or_ids)
            return_single = False

        if klass.table:
            ids = [klass.get_long_uid(uid) for uid in ids]

        kinds = set(klass.get_kind(uid) for uid in ids)
        if len(kinds) != 1:
            raise Exception("Can't get mixed kinds from SqlModel.")

        kind = kinds.pop()
        table = util.camel_to_separated(kind, sep='_').lower()

        query = '''
            SELECT *
            FROM {table}
            WHERE `uid` IN ({interp})
        '''.format(
            table=table,
            interp=','.join(['%s'] * len(ids)),
        )

        with mysql_connection.connect() as sql:
            dict_results = sql.select_query(query, tuple(ids))
            results = [klass.row_dict_to_obj(d) for d in dict_results]

        if return_single:
            return results[0] if results else None
        else:
            return results

    @classmethod
    def count(klass, **kwargs):
        with mysql_connection.connect() as sql:
            num = sql.count_where(klass.table, **kwargs)
        return num

    @classmethod
    def select(klass, limit=100, offset=None, order_by=None, descending=False,
               **where_params):
        """Run a SELECT query on the table.

        Here for legacy interface until this becomes truly OO.
        """
        with mysql_connection.connect() as sql:
            results = sql.select_star_where(
                klass.table, limit=limit, offset=offset, order_by=order_by,
                descending=descending, **where_params
            )
        return [klass.row_dict_to_obj(d) for d in results]

    @classmethod
    def get(klass, **kwargs):
        # Change datastore dialect into sql dialect.
        if 'n' in kwargs:
            kwargs['limit'] = kwargs.pop('n')
        if 'cursor' in kwargs:
            kwargs['offset'] = int(kwargs.pop('cursor'))
        if 'order' in kwargs:
            order = kwargs.pop('order')
            # Translate a leading minus into descending=True
            if order.startswith('-'):
                kwargs['descending'] = True
                order = order[1:]
            kwargs['order_by'] = order

        results = CursorResult(klass.select(**kwargs))

        # If we're using a non-default limit (mysql_api has a default limit of
        # 100), then assume we want to return paging information, which takes
        # the form of integer offsets in terms of the SQL query, and in terms
        # of a result set cursor object in terms of our API.
        limit = kwargs.get('limit', None)
        if limit and limit != float('inf'):
            offset = kwargs.get('offset', None) or 0

            # Last: last multiple of limit less than the full length.
            #
            # | page boundary
            # - result object
            # ^ cursor
            #
            # | # zero results
            # ^
            #
            # |- -|  # two results, limit 2 (penultimate page boundary)
            # ^
            #
            # |- -|- -|  # four results, limit 2 (penultimate page boundary)
            #     ^
            #
            # |- -|-  # three results, limit 2 (last page boundary)
            #     ^

            # Drop the limit and offset, but keep all other query arguments to
            # calculate the full length of the results set.
            kwargs_for_count = kwargs.copy()
            kwargs_for_count.pop('limit', None)
            kwargs_for_count.pop('offset', None)
            kwargs_for_count.pop('order_by', None)
            results_len = klass.count(**kwargs_for_count)

            last_page = results_len / limit  # floor division!
            if results_len > 0 and results_len % limit == 0:
                # Last page boundary is end of result set, step back one page.
                last_page -= 1
            last = last_page * limit
            results.last_cursor = SqlCursor(last if last > 0 else 0)

            # Next: one page forward, or the last page.
            nex = offset + limit
            results.next_cursor = SqlCursor(nex if nex < last else last)

            # Previous: one page back, or the beginning.
            prev = offset - limit
            results.previous_cursor = SqlCursor(prev if prev > 0 else 0)

        return results

    @classmethod
    def row_dict_to_obj(klass, row_dict):
        """Convert db values (e.g. 0) to python values (e.g. False) and
        instantiate as an entity. To be used after reading."""
        return klass(**klass.coerce_row_dict(row_dict))

    @classmethod
    def coerce_row_dict(klass, row_dict):
        """Convert python values (e.g. False) to db values (e.g. 0).
        To be used before writing."""
        return klass.convert_bool_props(klass.convert_json_props(row_dict))

    @classmethod
    def strip_timestamps(klass, row_dict):
        """Don't save timestamps, the db schema takes care of them."""
        row_dict = row_dict.copy()
        row_dict.pop('modified', None)  # allow db to use CURRENT_TIMESTAMP
        row_dict.pop('created', None)  # we never want this to change, anyway
        return row_dict

    @classmethod
    @hooked_put
    def put_multi(klass, entities, *args, **kwargs):
        """Save multiple entities in a single INSERT query.

        Raises: MySQLdb.IntegrityError if any entity violates unique indexes
            other than uid.
        """
        if 'on_duplicate_key_update' in kwargs:
            raise Exception(
                "Can't use on_duplicate_key_update in SqlModel.put_multi().")

        if not isinstance(entities, (list, tuple)):
            entities = [entities]

        if len(entities) == 0:
            return

        row_dicts = [
            klass.strip_timestamps(klass.coerce_row_dict(e.to_dict()))
            for e in entities
        ]

        existing = klass.get_by_id([e.uid for e in entities])
        existing_ids = [e.uid for e in existing]
        to_insert = [r for r in row_dicts if r['uid'] not in existing_ids]
        to_update = [r for r in row_dicts if r['uid'] in existing_ids]
        affected_rows_insert = 0
        affected_rows_update = 0

        with mysql_connection.connect() as sql:
            if to_insert:
                affected_rows_insert = sql.insert_row_dicts(
                    klass.table, to_insert)
            for row_dict in to_update:
                uid = row_dict.pop('uid')
                affected_rows_update += sql.update_row(
                    klass.table, 'uid', uid, **row_dict)

        return affected_rows_insert + affected_rows_update

    @classmethod
    @hooked_put
    def put_for_index(klass, entity, index_name, *args, **kwargs):
        """Save to db, updating rows in place if they match the named index.

        Unlike put() and put_multi() b/c it doesn't necessarily raise an
        exception if the incoming entity matches a unique index.
        """
        indices = [i for i in klass.py_table_definition['indices']
                   if i.get('unique', False)]

        # Haven't thought through consequences of multiple unique indexes.
        if len(indices) != 1:
            raise Exception(
                "SqlModel.put_for_index requires exactly one unique index.")
        index = indices[0]

        # Protect against the schema changing and queries getting out of sync.
        if index_name != index['name']:
            raise Exception("Index name {} doesn't match table {}."
                            .format(index_name, klass.table))

        row_dict = klass.strip_timestamps(klass.coerce_row_dict(
            entity.to_dict()))

        # https://github.com/PERTS/gae_server/issues/20
        # https://github.com/PERTS/neptune/issues/1057
        forbidden_fields = ('uid', 'short_uid')
        on_duplicate_key_update = tuple(
            k for k in row_dict.keys() if k not in forbidden_fields)

        with mysql_connection.connect() as sql:
            affected_rows = sql.insert_or_update(
                klass.table,
                row_dict,
                on_duplicate_key_update,
            )

        if affected_rows == 1:
            # Entity was inserted, so written data has the same uid. Read it
            # out again to get db-supplied defaults like modified time.
            return klass.get_by_id(entity.uid)
        else:
            # Affected rows is 0 or 2, so the entity matched an existing uid.
            # Return it instead of the one we have.
            filters = {k: getattr(entity, k) for k in index['fields']}
            return klass.get(**filters)[0]

    @classmethod
    def delete_multi(klass, ids_or_entities):
        """Delete ids or sql entities.

        Args:
            ids_or_entities: list of either id strings or instances of SqlModel
        """
        if not isinstance(ids_or_entities, (list, tuple)):
            raise Exception(u"SqlModel.delete_multi() takes a list, got: {}"
                            .format(ids_or_entities))

        if len(ids_or_entities) == 0:
            return

        if all(isinstance(x, SqlModel) for x in ids_or_entities):
            ids = [x.uid for x in ids_or_entities]
        else:
            ids = ids_or_entities

        with mysql_connection.connect() as sql:
            affected_rows = sql.delete(klass.table, 'uid', ids)

        return affected_rows

    @classmethod
    def get_table_definition(klass):
        d = klass.py_table_definition

        fields = []
        for f in d['fields']:

            if f.default == klass.sql_null:
                # Our special null symbol object into SQL syntax.
                def_str = 'NULL'
            elif f.default == klass.sql_current_timestamp:
                # Our special current timestamp symbol object into SQL syntax.
                def_str = 'CURRENT_TIMESTAMP'
            elif isinstance(f.default, basestring):
                # Python strings get single quotes in SQL syntax.
                if "'" in f.default or '\\' in f.default:
                    raise Exception("Can't escape special chars in field's "
                                    "default value: {}".format(f.default))
                def_str = u"'{}'".format(f.default)
            elif type(f.default) in (int, float):
                # Python numbers don't need any dressing in SQL syntax.
                def_str = '{}'.format(f.default)
            elif f.default is not None:
                raise Exception("Can't process field's default value: {}"
                                .format(f.default))

            if f.on_update == klass.sql_current_timestamp:
                up_str = 'CURRENT_TIMESTAMP'

            s = u'`{name}` {type}{length} {unsigned} {null} {default} {on_update}'.format(
                name=f.name,
                type=f.type,
                length=u'({})'.format(f.length) if f.length else '',
                unsigned='unsigned' if f.unsigned else '',
                null='' if f.null else 'NOT NULL',
                default=u'DEFAULT {}'.format(def_str) if f.default is not None else '',
                on_update='ON UPDATE {}'.format(up_str) if f.on_update else '',
            )
            fields.append(s)

        indices = []
        for i in d['indices']:
            s = '{unique}INDEX `{name}` (`{fields}`)'.format(
                unique='UNIQUE ' if i.get('unique', False) else '',
                name=i['name'],
                fields='`, `'.join(i['fields'])
            )
            indices.append(s)

        charset = 'DEFAULT CHARSET={}'.format(d['charset']) \
            if 'charset' in d else ''
        collate = 'DEFAULT COLLATE={}'.format(d['collate']) \
            if 'collate' in d else ''


        dump = """CREATE TABLE `{table_name}` (
            {fields},
          PRIMARY KEY (`{primary_key}`)
          {indices}
        ) ENGINE={engine} {charset} {collate};
        """.format(
            table_name=d['table_name'],
            fields=',\n'.join(fields),
            primary_key='`, `'.join(d['primary_key']),
            indices=(',' + ',\n'.join(indices)) if indices else '',
            engine=d['engine'],
            charset=charset,
            collate=collate
        )
        return dump

    @classmethod
    def _field_data(klass, datatype, length, unsigned=None):
        if datatype == 'varchar':
            return ''.join(random.choice(string.ascii_lowercase)
                           for x in range(length))
        elif datatype == 'text':
            return ''.join(random.choice(string.ascii_lowercase)
                           # for x in range(1000))
                           for x in range(100))
        elif datatype.endswith('int'):
            x = int(math.floor(random.random() * (2 ** length)))
            if unsigned is False:
                x = x - (2 ** length) / 2
            return x
        elif datatype == 'datetime':
            return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise Exception("Type {} not yet implemented.".format(datatype))

    @classmethod
    def generate_test_data(klass, num_rows):
        row_dicts = [
            {
                f.name: klass._field_data(f.type, f.length, f.unsigned)
                for f in klass.py_table_definition['fields']
            }
            for x in range(num_rows)
        ]
        return row_dicts

    @classmethod
    def row_dicts_to_insert_query(klass, row_dicts):
        """Not the same as mysql_api.insert_row_dicts, because that uses
        MySQLdb to a bunch of the escaping and interpolation."""

        # Turn each row dictionary into an ordered dictionary
        ordered_rows = [OrderedDict(
            sorted(d.items(), key=lambda t: t[0])) for d in row_dicts]

        return "INSERT INTO `{table}` (`{fields}`) VALUES {rows}".format(
            table=klass.table,
            # Quote each field name in backticks.
            fields='`, `'.join(ordered_rows[0].keys()),
            # Comma-separated list of sets of row values.
            rows=','.join([
                # Paren-wrapped comma-separated list of row values, each
                # property quoted if necessary.
                '({})'.format(
                    ', '.join([escape(v, conversions) for v in r.values()])
                )
                for r in ordered_rows
            ])
        )

    @classmethod
    def convert_json_props(klass, row_dict):
        """Doesn't handle JSON string values, e.g. if the _parsed_ version of
        the value is a python string.
        """
        if type(row_dict) is not dict:
            raise Exception("SqlModel.convert_json_props got a bad value: {}"
                            .format(row_dict))
        new_dict = row_dict.copy()
        for k in getattr(klass, 'json_props', []):
            if k not in row_dict:
                continue
            v = row_dict[k]
            if isinstance(v, basestring):
                new_dict[k] = json.loads(v)
            else:
                new_dict[k] = json.dumps(v, default=util.json_dumps_default)
        return new_dict

    @classmethod
    def convert_bool_props(klass, row_dict):
        if type(row_dict) is not dict:
            raise Exception("SqlModel.convert_bool_props got a bad value: {}"
                            .format(row_dict))
        new_dict = row_dict.copy()
        bool_props = [f.name for f in klass.py_table_definition['fields']
                      if f.type.lower() in ('bool', 'boolean')]
        for k in bool_props:
            if k not in row_dict:
                continue
            v = row_dict[k]
            if isinstance(v, numbers.Number):
                # Convert from db values to pythonic bools.
                new_dict[k] = True if v == 1 else False
            else:
                # Convert from pythonic bools to db values.
                new_dict[k] = 1 if v is True else 0
        return new_dict

    def __init__(self, **kwargs):
        self._init_kwargs = kwargs

        for k, v in kwargs.items():
            setattr(self, k, v)

    def before_put(self, init_kwargs, *args, **kwargs):
        pass

    def after_put(self, init_kwargs, *args, **kwargs):
        pass

    @hooked_put
    def put(self, *args, **kwargs):
        """Save an object, whether or not the db has seen it yet.

        N.B. ignores the entity's modified time so the db can update it.

        Raises: MySQLdb.IntegrityError if the entity violates a unique index
            other than uid.
        """

        row_dict = self.strip_timestamps(self.coerce_row_dict(self.to_dict()))
        with mysql_connection.connect() as sql:
            affected_rows = sql.insert_or_update(self.table, row_dict)

        # We can't predict exactly what the values stored in the db will be
        # for fields that default to current timestamp. So after writing, get
        # the row and refresh this object with the data.
        if affected_rows == 1:
            fetched = self.get_by_id(self.uid)
            self.__init__(**fetched.to_dict())

    def to_dict(self):
        return {f.name: getattr(self, f.name)
                for f in self.py_table_definition['fields']
                if hasattr(self, f.name)}

    def to_client_dict(self):
        """The same as to_dict for now, but behavior may diverge."""
        d = self.to_dict()
        return OrderedDict((k, d[k]) for k in sorted(d.keys()))

    def __str__(self):
        """A string represenation of the entity. Goal is to be readable.

        Returns, e.g. <id_model.User User_oha4tp8a>.
        Native implementation does nothing useful.
        """
        return '<{}>'.format(self.uid)

    def __repr__(self):
        """A unique representation of the entity. Goal is to be unambiguous.

        But our ids are unambiguous, so we can just forward to __str__.

        Native implemention returns a useless memory address, e.g.
            <id_model.User 0xa5e418cdd>
        The big benefit here is to be able to print debuggable lists of
        entities, without need to manipulate them first, e.g.
            print [entity.id for entity in entity_list]
        Now you can just write
            print entity_list
        """
        return self.__str__()

    # special methods to allow comparing entities, even if they're different
    # instances according to python
    # https://groups.google.com/forum/?fromgroups=#!topic/google-appengine-python/uYneYzhIEzY
    def __eq__(self, value):
        """Allows entity == entity to be True if keys match.

        Is NOT called by `foo is bar`."""
        if self.__class__ == value.__class__:
            return self.uid == value.uid
        else:
            return False

    # Because we defined the 'equals' method, eq, we must also be sure to
    # define the 'not equals' method, ne, otherwise you might get A == B is
    # True, and A != B is also True!
    def __ne__(self, value):
        """Allows entity != entity to be False if keys match."""
        if self.__class__ == value.__class__:
            # if they're the same class, compare their keys
            return self.uid != value.uid
        else:
            # if they're not the same class, then it's definitely True that
            # they're not equal.
            return True

    def __hash__(self):
        """Allows entity in entity_list to be True."""
        return hash(str(self.uid))


SqlModel.convert_uid = DatastoreModel.__dict__['convert_uid']
SqlModel.generate_uid = DatastoreModel.__dict__['generate_uid']
SqlModel.get_kind = DatastoreModel.__dict__['get_kind']
SqlModel.get_long_uid = DatastoreModel.__dict__['get_long_uid']
SqlModel.get_url_kind = DatastoreModel.__dict__['get_url_kind']
SqlModel.id_pattern = DatastoreModel.__dict__['id_pattern']
SqlModel.id_regex = DatastoreModel.__dict__['id_regex']
SqlModel.is_long_uid = DatastoreModel.__dict__['is_long_uid']
SqlModel.is_short_uid = DatastoreModel.__dict__['is_short_uid']
SqlModel.kind_pattern = DatastoreModel.__dict__['kind_pattern']
SqlModel.kind_to_class = DatastoreModel.__dict__['kind_to_class']
SqlModel.long_uid_part_regex = DatastoreModel.__dict__['long_uid_part_regex']
