class CursorResult(list):
    """A list with optional meta data.

    Makes returning datastore results more flexible because the list of results
    is immediately usable, but cursor-aware queries also get what they need.

    Example:
        r = User.get(n=10)
        len(n)  # 10
        isinstance(r, list)  # True
        r.next_cursor  # Cursor object
    """
    # Supply curors as argument to DatastoreModel.get to get the next or
    # previous page of results.
    next_cursor = None
    previous_cursor = None
    last_cursor = None  # only used for SQL models
    more = None


class SqlCursor(object):
    """Imitates the interface of a Datastore cursor."""
    def __init__(self, offset=None):
        if offset is not None and type(offset) is not int:
            raise Exception("SqlCursor only accepts integers.")
        self.offset = offset

    def __int__(self):
        """Treat it like an integer offset in a SQL query."""
        return self.offset

    def __bool__(self):
        """Test if it's empty, but treat zero as non-empty."""
        return self.offset is not None

    def urlsafe(self):
        """Use it like a Datastore Cursor in URLs."""
        return self.offset
