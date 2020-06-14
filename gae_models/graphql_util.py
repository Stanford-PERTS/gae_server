from graphql.language import ast
import graphene
import logging

from gae_models import CachedPropertiesModel
import config
import util


def resolve_client_prop(prop, default, root, info):
    val_found = False

    # Check for the requested data in three places, in order:

    # 1. The entity itself (object attribute)
    if hasattr(root, prop):
        val = getattr(root, prop)
        val_found = True

    # 2. Cached properties (dictionary in memcache)
    if not val_found and isinstance(root, CachedPropertiesModel):
        # Avoid hitting memcache many times.
        if not getattr(root, '_cached_properties', None):
            root._cached_properties = root.get_cached_properties()
        if prop in root._cached_properties:
            val = root._cached_properties[prop]
            val_found = True

    # 3. The client dictionary
    if not val_found:
        # Avoid creating the client dict many times.
        if not getattr(root, '_client_dict', None):
            root._client_dict = root.to_client_dict()
        if prop in root._client_dict:
            val = root._client_dict[prop]
            val_found = True

    if not val_found:
        raise Exception("Could not find property {} of {}".format(prop, root))

    return val


class DatastoreDateTimeScalar(graphene.Scalar):
    @staticmethod
    def serialize(dt):
        return util.datelike_to_iso_string(dt) if dt else None

    @classmethod
    def parse_literal(klass, node):
        if isinstance(node, ast.StringValue):
            return klass.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        try:
            return value.strptime(config.iso_datetime_format)
        except:
            return None


class DatastoreDateScalar(graphene.Scalar):
    @staticmethod
    def serialize(d):
        return util.datelike_to_iso_string(d) if d else None

    @classmethod
    def parse_literal(klass, node):
        if isinstance(node, ast.StringValue):
            return klass.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        try:
            return value.strptime(config.iso_date_format)
        except:
            return None


class PassthroughScalar(graphene.Scalar):
    @staticmethod
    def serialize(x):
        return x

    @classmethod
    def parse_literal(klass, node):
        if isinstance(node, ast.StringValue):
            return klass.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        return value


# Chose bettter graphql types than graphene.NdbObjectType does.
class DatastoreType(graphene.Interface):
    uid = graphene.ID()
    short_uid = graphene.ID()
    created = DatastoreDateTimeScalar()
    modified = DatastoreDateTimeScalar()


class SqlType(graphene.Interface):
    uid = graphene.ID()
    short_uid = graphene.ID()
