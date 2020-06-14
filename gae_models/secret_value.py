"""
SecretValue Model
===========

Class representing an secret key-value pair.
"""


from google.appengine.ext import ndb


class SecretValue(ndb.Model):
    """A secret key-value pair.

    Currently used for storing configuration values, like hash salts.
    """
    value = ndb.TextProperty(default='')

    @classmethod
    def get(klass, id, *args):
        """Get the value stored in a secret value entity, by id.

        Args:
            id - str, datastore id/keyname
            default - mixed, optional, value to use if the entity is not found

        Raises exception if entity is not found and there's no default.
        """
        sv = klass.get_by_id(id)
        if sv is None:
            if len(args) == 0:
                raise Exception("SecretValue with id {} not found.".format(id))
            elif len(args) == 1:
                return args[0]  # default value
            else:
                raise Exception("SecretValue.get() takes at most 2 arguments")
        else:
            return sv.value
