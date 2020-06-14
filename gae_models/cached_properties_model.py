from google.appengine.api import memcache

import util


class CachedPropertiesModel(object):
    """An entity associated with a memcache key that may contain a flat dict."""

    @classmethod
    def batch_cached_properties_from_db(klass, ids):
        """Calls instance method get_cached_properties()."""
        raise NotImplementedError("{}.get_cached_properties_from_db()"
                                  .format(klass.__name__))

    def get_cached_properties_from_db(self):
        """Returns a flat dict."""
        raise NotImplementedError("{}.get_cached_properties_from_db()"
                                  .format(klass.__name__))

    def update_cached_properties(self):
        """Refreshes the value of cached properties in memcache."""
        from_db = self.get_cached_properties_from_db()
        if from_db:
            memcache.set(util.cached_properties_key(self.uid), from_db)
        return from_db

    def get_cached_properties(self):
        """Get cached properties, defaulting to the db if necessary."""
        cached = memcache.get(util.cached_properties_key(self.uid))
        if cached:
            return cached
        return self.update_cached_properties()
