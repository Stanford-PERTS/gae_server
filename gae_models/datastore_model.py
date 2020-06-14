"""
DatastoreModel
===========

Superclass for all other datastore-backed models;
contains generic properties and methods
"""

from google.appengine.api import memcache
from google.appengine.ext import ndb
import collections
import datetime
import logging
import os_random
import re
import string
import sys

from .cursor import CursorResult
from .model_util import reverse_order_str
import util


class DatastoreModel(ndb.Model):
    """Superclass for all others; contains generic properties and methods."""

    # This uid is an encoding of the entity's key. For root entities (those
    # with no ancestors), it's a class name (same as a GAE "kind") and
    # an identifier separated by an underscore.
    # Example: Theme_mW4iQ4cO
    # For entities in a group (those with ancestors), their full heirarchy is
    # encoded, ending with the root entity, separated by periods.
    # Example: Comment_p46aOHS6.User_80h41Q4c
    # A uid is always sufficient to look up an entity directly.
    # N.B.: We can't just call it 'id' because that breaks ndb.
    uid = ndb.ComputedProperty(lambda self: self.key.id())
    short_uid = ndb.ComputedProperty(
        lambda self: self.convert_uid(self.key.id()))
    deleted = ndb.BooleanProperty(default=False)
    created = ndb.DateTimeProperty(auto_now_add=True)
    modified = ndb.DateTimeProperty(auto_now=True)

    id_length = 8

    # Letters only, in StandingCamelCase
    kind_pattern = r'([A-Z][a-z]+)+'

    # Letters, numbers, hyphen.
    id_pattern = r'[A-Za-z0-9\-]+'

    # * Group 1 is the kind
    # * Group 3 is the id
    # * The whole pattern is the "part"
    # A valid long uid is one or more parts separated by dots.
    long_uid_part_regex = re.compile(
        '^({kind})_({id})$'.format(kind=kind_pattern, id=id_pattern))

    # Short uids just have ids separated by dots; no kind or underscore.
    id_regex = re.compile('^{id}$'.format(id=id_pattern))

    @classmethod
    def property_types(klass):
        """Get a map of property names to their expected types.

        Controls what can be queried in a query string and can be written to in
        a PUT. Designed to play nice with ApiHandler.get_params().
        """
        # All attributes.
        attributes = [getattr(klass, a) for a in dir(klass)]
        # Only those attributes which are editable model properties.
        not_editable = (ndb.ModelKey, ndb.ComputedProperty)
        properties = [
            a for a in attributes
            if isinstance(a, ndb.Property) and not isinstance(a, not_editable)
        ]

        property_types = {}
        for p in properties:
            typ = None
            if p._name in ['uid', 'created', 'modified']:
                # These are read-only from the client's perspective, so they
                # have no updatable "type".
                continue
            elif getattr(p, '_repeated', None):
                typ = list
            elif isinstance(p, ndb.StringProperty):
                typ = str
            elif isinstance(p, ndb.TextProperty):
                typ = unicode
            elif isinstance(p, ndb.BooleanProperty):
                typ = bool
            elif isinstance(p, ndb.IntegerProperty):
                typ = int
            elif isinstance(p, ndb.DateProperty):
                typ = 'date'
            elif isinstance(p, ndb.DateTimeProperty):
                typ = 'datetime'

            if typ:
                property_types[p._name] = typ

        if hasattr(klass, 'json_props'):
            # Trim the '_json' ending.
            property_types.update({
                p[:-5]: 'json' for p in klass.json_props
                if p in property_types.keys()
            })

            # Delete the text-based json properties, since we don't want the
            # client to access them directly.
            for p in klass.json_props:
                property_types.pop(p, None)

        return property_types

    @classmethod
    def create(klass, **kwargs):
        # ndb expects parents to be set with a key, but we find it more
        # convenient to pass in entities. Do the translation here.
        if 'parent' in kwargs:
            parent = kwargs['parent']  # used in id generation
            if isinstance(parent, ndb.Model):
                kwargs['parent'] = kwargs['parent'].key
            elif not isinstance(parent, ndb.Key):
                raise Exception("Parent must be Key or DatastoreModel. Got: {}"
                                .format(parent))
        else:
            parent = None

        if 'id' in kwargs:
            # User has supplied their own id. That's okay (it makes certain
            # URLs nice and readable), but it's not a real uid yet b/c it
            # doesn't adhere to our ClassName_identifierXYZ convention. We'll
            # pass it into generated_uid() later.
            identifier = kwargs['id']
            del kwargs['id']
        else:
            identifier = None

        # Make sure id is unique, otherwise "creating" this entity will
        # overwrite an existing one, which could be a VERY hard bug to chase
        # down.
        for x in range(5):
            uid = klass.generate_uid(parent, identifier)
            existing_entity = klass.get_by_id(uid)
            if not existing_entity:
                break
        if existing_entity:
            if identifier:
                raise Exception("Entity {} already exists.".format(uid))
            else:
                raise Exception("After five tries, could not generate a "
                                "unique id. This should NEVER happen.")

        # Any json properties can't be set directly in the constructor, but
        # have to be added later.
        if hasattr(klass, 'json_props'):
            json_kwargs = {}  # Json kwargs will be stashed here temporarily.
            for json_k in klass.json_props:
                # Switch from the ndb.TextProperty to the @property.
                k = json_k[:-5]
                # If this property is specified in kwargs, stash it.
                if k in kwargs:
                    json_kwargs[k] = kwargs.pop(k)

        new_entity = klass(id=uid, **kwargs)

        # Go back and set any json kwargs that were stashed.
        if hasattr(klass, 'json_props'):
            for k, v in json_kwargs.items():
                setattr(new_entity, k, v)

        return new_entity

    @classmethod
    def generate_uid(klass, parent=None, identifier=None, existing=False):
        """Make a gobally unique id string, e.g. 'Program_mW4iQ4cO'.

        Using 8 random chars, if we made 10,000 entities of the same kind, the
        probability of duplication is 2E-7. Combined with the five attempts at
        uniqueness in DatastoreModel.create(), chances of duplication are
        essentially nil.
        http://en.wikipedia.org/wiki/Universally_unique_identifier#Random_UUID_probability_of_duplicates

        If a parent entity is specified, it becomes part of the uid, like this:
        Comment_p46aOHS6.User_80h41Q4c

        If an identifier is specified, it is used instead of random characters:
        Theme_growth-mindset-is-cool
        """
        if identifier:
            if not klass.id_regex.match(identifier):
                raise Exception("Invalid identifier: {}. Letters, numbers, "
                                "and hyphens only.".format(identifier))
            suffix = identifier
        else:
            c = (string.ascii_uppercase + string.ascii_lowercase +
                 string.digits)
            suffix = ''.join(os_random.choice(c)
                             for x in range(klass.id_length))
        uid = klass.__name__ + '_' + suffix

        # Because comments exist as descendants of other entities, a simple
        # id-as-key-name is insufficient. We must store information about its
        # ancestry as well. Example:
        # Comment_p46aOHS6.User_80h41Q4c
        if parent and isinstance(parent, ndb.Model):
            uid += '.' + parent.uid
        elif parent and isinstance(parent, ndb.Key):
            uid += '.' + parent.id()

        return uid

    @classmethod
    def is_long_uid(klass, short_or_long_uid):
        parts = short_or_long_uid.split('.')
        return all(klass.long_uid_part_regex.match(p) for p in parts)

    @classmethod
    def is_short_uid(klass, short_or_long_uid):
        parts = short_or_long_uid.split('.')
        return all(klass.id_regex.match(p) for p in parts)

    @classmethod
    def convert_uid(klass, short_or_long_uid):
        """Changes long-form uid's to short ones, and vice versa.

        Long form example: Theme_growth-mindset-is-cool
        Short form exmaple: growth-mindset-is-cool
        """
        if klass.is_long_uid(short_or_long_uid):
            parts = short_or_long_uid.split('.')
            return '.'.join([p.split('_')[1] for p in parts])
        elif klass.is_short_uid(short_or_long_uid):
            return klass.get_long_uid(short_or_long_uid)
        else:
            # See comments on returning None in get_long_uid().
            return None

    @classmethod
    def get_long_uid(klass, short_or_long_uid, kinds=None):
        """Changes short or long-form uid's to long ones.

        Long form example: Theme_growth-mindset-is-cool
        Short form exmaple: growth-mindset-is-cool

        Note that short uids must be converted by their class,
        e.g. User.get_long_uid('80h41Q4c') b/c otherwise we don't know the
        kind.

        Note further that classes which have ancestors must override
        get_long_uid() so they can supply ancestor kinds as well,
        e.g. Comment.get_long_uid('p46aOHS6', kinds=('Comment', 'User'))

        Returns: long uid str or None. Doesn't raise anything if the uid is
        invalid because we want to imitate ndb's behavior when getting a key
        that doesn't exist in the datastore: it merely returns None.
        e.g. User.get_by_id(some_invalid_str) should return None even if the
        string can't be matched, or split, or is the wrong length, etc.
        """
        kinds = kinds or (klass.__name__,)

        if not short_or_long_uid:
            return None

        if klass.is_long_uid(short_or_long_uid):
            return short_or_long_uid  # already long
        elif klass.is_short_uid(short_or_long_uid):
            ids = short_or_long_uid.split('.')  # always length >= 1
            if len(ids) == len(kinds):
                return '.'.join('{kind}_{id}'.format(kind=k, id=i)
                                for k, i in zip(kinds, ids))
            else:
                logging.warning(
                    "Can't convert short uids that have ancestors without "
                    "knowing the ancestor kinds."
                )

        return None

    @classmethod
    def get_parent_uid(klass, uid):
        """Don't use the datastore; get parent ids based on convention."""
        if '.' not in uid:
            raise Exception("Can't get parent of id: {}".format(uid))
        return '.'.join(uid.split('.')[1:])

    @classmethod
    def get_kind(klass, obj):
        """Get the kind (class name string) of an entity, key, or id.

        Examples:
        * For a Theme entity, the kind is 'Theme'
        * For the id 'Comment_p46aOHS6.User_80h41Q4c',
          the kind is 'Comment'.
        """
        if isinstance(obj, basestring):
            return str(obj.split('_')[0])
        elif isinstance(obj, ndb.Key):
            return obj.kind()
        else:
            return obj.__class__.__name__

    @classmethod
    def get_url_kind(klass, obj):
        """Get the url kind (lowercase plural string) of an entity, key, or id.

        Examples:
        * For a Theme entity, the url kind is 'themes'
        * For a DataTable entity, the url kind is 'data_tables'
        """
        kind = klass.get_kind(obj)
        return util.camel_to_separated(kind, sep='_').lower() + 's'

    @classmethod
    def kind_to_class(klass, kind):
        """Convert a class name string (same as a GAE kind) to a class.

        See http://stackoverflow.com/questions/1176136/convert-string-to-python-class-object
        """
        return getattr(sys.modules['model'], kind, None)

    @classmethod
    def url_kind_to_class(klass, url_kind):
        s = url_kind[:-1]  # remove trailing 's'
        kind = util.separated_to_camel(s, sep='_', standing=True)
        return klass.kind_to_class(kind)

    @classmethod
    def id_to_key(klass, id):
        long_uid = klass.get_long_uid(id)
        if not long_uid:
            # See comments on returning None in get_long_uid().
            return None
        parts = long_uid.split('.')
        pairs = [(klass.get_kind(p), '.'.join(parts[-x:]))
                 for x, p in enumerate(parts)]
        return ndb.Key(pairs=reversed(pairs))

    @classmethod
    def get_by_id(klass, id_or_ids):
        """The main way to get **non-deleted** entities with known ids.

        If you want to get an entity regardless of deleted status, use
        DatastoreModel.id_to_key(uid).get()

        Args:
            id_or_ids: A single perts id string, or an iterable of such strings,
                of any kind or mixed kinds.
        Returns an entity or list of entities, depending on input.
        """

        # Sanitize input to a list of strings.
        if type(id_or_ids) in [str, unicode]:
            ids = [id_or_ids]
        elif isinstance(id_or_ids, collections.Iterable):
            ids = id_or_ids
        else:
            # I don't think we should be blocking code here
            # Problem was occuring when you search a bad id or just None
            # Ex. "/topics/foobar."
            return None

        keys = [klass.id_to_key(id) for id in ids]
        logging.info("{}.get_by_id({})".format(klass.__name__, keys))
        results = ndb.get_multi(keys)

        # Filter out deleted entities.
        results = [e for e in results if e and e.deleted is False]

        # Wrangle results into expected structure.
        if type(id_or_ids) in [str, unicode]:
            if len(results) > 0:
                return results[0]
            else:
                return None
        if isinstance(id_or_ids, collections.Iterable):
            return results

    @classmethod
    def limit_subqueries(klass, filters):
        # GAE limits us to 30 subqueries! This is a BIG problem, because
        # stacking 'property IN list' filters MULTIPLIES the number of
        # subqueries (since IN is shorthand for a bunch of = comparisions). My
        # temporary solution is to detect unwieldy queries and do some post-
        # processing in python.
        # https://groups.google.com/forum/#!topic/google-appengine-python/ZlqZHwfznbQ
        subqueries = 1
        safe_filters = {}
        unsafe_filters = {}
        in_filters = {}
        for k, v in filters.items():
            if type(v) is list:
                subqueries *= len(v)
                in_filters[k] = v
            else:
                safe_filters[k] = v
        if subqueries > 30:
            # mark in_filters as unsafe one by one, starting with the largest,
            # until subqueries is small enough
            s = subqueries
            for k, v in sorted(in_filters.items(), key=lambda f: len(f[1]),
                               reverse=True):
                if s < 30:
                    safe_filters[k] = v
                else:
                    unsafe_filters[k] = v
                s /= len(v)
        else:
            safe_filters.update(in_filters)
        if len(unsafe_filters) > 0:
            logging.info(u'DatastoreModel.limit_subqueries() marked filters '
                         'as unsafe because they would generate too many '
                         'subqueries:')
            logging.info(u'{}'.format(unsafe_filters))
        return (safe_filters, unsafe_filters)

    @classmethod
    def post_process(klass, results, unsafe_filters):
        """Assumes IN filters with list values, e.g. {'id', ['X', 'Y']}."""
        logging.info(u'DatastoreModel.post_process() handled unsafe filters:')
        logging.info(u'{}'.format(unsafe_filters))
        all_matching_sets = []
        for k, v in unsafe_filters.items():
            matches = set([e for e in results if getattr(e, k) in v])
            all_matching_sets.append(matches)
        return set.intersection(*all_matching_sets)

    @classmethod
    def _query(klass, ancestor=None, order=None, **kwargs):
        logging.info(u'{}.query(ancestor={}, order={}, kwargs={})'
                     .format(klass.__name__, ancestor, order, kwargs))

        if ancestor:
            # @todo: permissions here?
            # if not self.user:
            #     raise PermissionDenied("Public cannot run ancestor queries.")
            # elif not self.user.is_admin and ancestor != self.user:
            #     raise PermissionDenied(
            #         "Users can only run ancestor queries on themselves.")

            # Convert string id or entity to key.
            if type(ancestor) is str:
                ancestor = DatastoreModel.id_to_key(ancestor)
            if isinstance(ancestor, DatastoreModel):
                ancestor = ancestor.key
            query = klass.query(klass.deleted == False, ancestor=ancestor)
        else:
            query = klass.query(klass.deleted == False)

        if order:
            query = query.order(klass.convert_order_str(order))

        safe_kwargs, unsafe_kwargs = klass.limit_subqueries(kwargs)

        for k, v in safe_kwargs.items():
            if type(v) is list:
                if len(v) == 0:
                    raise Exception("Cannot filter query on empty list.")
                query = query.filter(getattr(klass, k).IN(v))
            elif k.endswith('!'):
                k = k[:-1]
                query = query.filter(getattr(klass, k) != v)
            else:
                query = query.filter(getattr(klass, k) == v)

        query.safe_kwargs = safe_kwargs
        query.unsafe_kwargs = unsafe_kwargs

        return query

    @classmethod
    def convert_order_str(klass, order_str):
        # Uses '-' in order to indicate reverse direction, otherwise standard
        # sorting is used.
        if order_str == '':
            return klass.key
        elif order_str == '-':
            return -klass.key
        elif order_str.startswith('-'):
            return -getattr(klass, order_str[1:])
        else:
            return getattr(klass, order_str)

    @classmethod
    def get(klass, n=10, ancestor=None, order=None, cursor=None,
            projection=None, keys_only=None, **filter_kwargs):
        """Query entities in the datastore.

        Makes either strongly consistent or eventually consistent queries based
        on the supplied parameters.

        Args:
            n: int or infinity, default 10, maximum number to return.
            ancestor: entity, if specified query is strongly consistent,
                otherwise it is eventually consistent.
            order: str, property name, possibly prepended with a minus to
                represent reverse ordering.
            cursor: obj, black-box value to start result set where a previous
                set left off.
            projection: iterable of str, if set only these named properties are
                returned for each entity
            keys_only: bool, if True keys are returned rather than entities
            filter_kwargs: keyword arguments to filter the query; see below.

        Filtering arguments (anything gathered into filter_kwargs above) can
        be postpended with an exclamation mark to indicate not-equal, e.g.
        {'status!': 'rejected'} queries for entities whose status property does
        not have the value 'rejected'.

        CAUTION: for complicated, annoying, interacting reason, we cannot
        combine finite n with any filters other than equality (==) and
        membership (IN), e.g. ?n=10&status!=rejected doesn't work. And only
        super admins can run inifite queries, so only super admins can use
        negative filters.

        Returns:
            generator via query.iter() if n is infinite
            CursorResult if n is finite; see that class for details.
        """
        logging.info(u'{}.get(n={}, ancestor={}, order={}, cursor={}, '
                     'projection={}, keys_only={}, filter_kwargs={})'
                     .format(klass.__name__, n, ancestor, order, cursor,
                             projection, keys_only, filter_kwargs))

        # @todo: permissions here?
        # if n > 10 and not self.user:
        #     raise PermissionDenied("Public cannot change result set size.")

        # Uniquify any requested ids. Allows other code to be lazy, and may
        # save on subqueries.
        if 'uid' in filter_kwargs and type(filter_kwargs['uid']) is list:
            filter_kwargs['uid'] = list(set(filter_kwargs['uid']))

        fetch_kwargs = {}

        if projection is not None:
            # Change from string names of attributes to properties themselves.
            fetch_kwargs['projection'] = [getattr(klass, p)
                                          for p in projection]
        if keys_only is not None:
            fetch_kwargs['keys_only'] = keys_only
        if cursor is not None:
            fetch_kwargs['start_cursor'] = cursor

        if n == float('inf'):
            query = klass._query(ancestor=ancestor, order=order,
                                 **filter_kwargs)
            results = query.iter(**fetch_kwargs)
        else:
            # To do paging with reverse cursors, there must be an order. If
            # none specified, use the default: klass.key represented by ''.
            order_str = '' if order is None else order
            order = klass.convert_order_str(order_str)
            reverse_order = klass.convert_order_str(
                reverse_order_str(order_str))

            query = klass._query(ancestor=ancestor, **filter_kwargs)
            f_query = query.order(order)
            r_query = query.order(reverse_order)
            f_results, f_cursor, f_more = f_query.fetch_page(n, **fetch_kwargs)
            r_results, r_cursor, r_more = r_query.fetch_page(n, **fetch_kwargs)

            results = CursorResult(f_results)
            # Cursors are None if results are empty.
            results.next_cursor = f_cursor
            results.previous_cursor = r_cursor
            results.more = f_more

        # post-processing, if necessary
        if len(query.unsafe_kwargs) > 0:
            results = klass.post_process(results, query.unsafe_kwargs)

        return results

    @classmethod
    def count(klass, ancestor=None, **filter_kwargs):
        query = klass._query(ancestor=ancestor, **filter_kwargs)
        if len(query.unsafe_kwargs) > 0:
            raise Exception("Specified filters generate > 30 subqueries.")
        return query.count()

    def before_put(self, *args, **kwargs):
        """Process entities before saving to datastore."""
        # Override this method in inheriting models.
        pass

    def after_put(self, *args, **kwargs):
        """Process entities after saving to datastore."""
        # Override this method in inheriting models.
        pass

    def put(self, *args, **kwargs):
        """Hook into the normal my_entity.put() operations to add more options.

        Args:
            **kwargs: normal app engine arguments, see docs:
                https://developers.google.com/appengine/docs/python/datastore/modelclass#Model_put
        """
        self.before_put(*args, **kwargs)

        super(DatastoreModel, self).put(*args, **kwargs)

        self.after_put(*args, **kwargs)

    def __str__(self):
        """A string represenation of the entity. Goal is to be readable.

        Returns, e.g. <id_model.User User_oha4tp8a>.
        Native implementation does nothing useful.
        """
        return '<{}>'.format(self.key.id())

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
            return self.key.id() == value.key.id()
        else:
            return False

    # Because we defined the 'equals' method, eq, we must also be sure to
    # define the 'not equals' method, ne, otherwise you might get A == B is
    # True, and A != B is also True!
    def __ne__(self, value):
        """Allows entity != entity to be False if keys match."""
        if self.__class__ == value.__class__:
            # if they're the same class, compare their keys
            return self.key.id() != value.key.id()
        else:
            # if they're not the same class, then it's definitely True that
            # they're not equal.
            return True

    def __hash__(self):
        """Allows entity in entity_list to be True."""
        return hash(str(self.key))

    def to_client_dict(self, override=None):
        """Convert an app engine entity to a dictionary.

        Ndb provides a to_dict() method, but we want to add creature-features:
        1. Put properties in a predictable order so they're easier to read.
        2. Remove or mask certain properties based on the preferences of our
           javascript.
        3. Handle our string-based json_properties correctly.
        4. Ndb (different from db) stores datetimes as true python datetimes,
           which JSON.dumps() doesn't know how to handle. We'll convert them
           to ISO strings (e.g. "2010-04-20T20:08:21Z")

        Args:
            override: obj, if provided, method turns this object into
                a dictionary, rather than self.
        """
        output = self.to_dict()

        if hasattr(self, 'json_props'):
            # Remove any JSON text properties and replace with their parsed
            # version.
            for p in self.json_props:
                del output[p]
                output[p[:-5]] = getattr(self, p[:-5])

        for k, v in output.items():
            if isinstance(v, datetime.date):
                # This isinstance test covers both dates and datetimes.
                output[k] = util.datelike_to_iso_string(v)

        # order them so they're easier to read
        ordered_dict = collections.OrderedDict(
            sorted(output.items(), key=lambda t: t[0]))

        return ordered_dict

# Hook into ndb.put_multi, just like we hooked into db.Model.put
# We'll need a reference to the original function so we can use it within
# the new monkeypatched function.
_old_put_multi = ndb.put_multi


def _hooked_put(entities, memcache_management=True, *args, **kwargs):
    """Replacement of ndb.put_multi so we can hook in additional logic.

    Args same as standard ndb.put_multi, but with additional arguments:
        memcache_management: bool, skip any PERTS-created memecache management.

    See https://developers.google.com/appengine/docs/python/datastore/functions#put
    """
    # Only run before_put() if these are lists of DatastoreModel entities.
    # MapReduce, for instance, uses this code with entities that don't have
    # before_put defined, and so we should let them through silently.
    for e in entities:
        if hasattr(e, 'before_put'):
            e.before_put(memcache_management=memcache_management, *args,
                         **kwargs)

    # Put the original arguments (but not the ones we've added) to preserve
    # default behavior.
    _old_put_multi(entities, *args, **kwargs)

    for e in entities:
        if hasattr(e, 'after_put'):
            e.after_put(memcache_management=memcache_management, *args,
                        **kwargs)


ndb.put_multi = _hooked_put
