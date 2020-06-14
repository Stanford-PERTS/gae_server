"""
StorageObject
===========

Generic metadata for Google Cloud Storage files.
"""

from google.appengine.ext import ndb

from gae_models import DatastoreModel
import cloudstorage as gcs


class StorageObject(DatastoreModel):
    """Generic metadata for Google Cloud Storage files."""

    # Owner/uploader.
    user_id = ndb.StringProperty()
    # Name as uploaded, set in header when downloaded.
    filename = ndb.StringProperty()
    # Path to file on Google Cloud Storage:
    # r'^/(?P<bucket>.+)/data_tables/(?P<object>.+)$'
    gcs_path = ndb.StringProperty()
    # Size of the file, in bytes.
    size = ndb.IntegerProperty()  # watch out, might be a python long, not int
    # Content type (example: 'text/csv')
    content_type = ndb.StringProperty()

    @property
    def gs_object_name(self):
        """Used in blobstore.create_gs_key for direct-to-storage upload links"""
        return '/gs' + self.gcs_path

    def read(self):
        with gcs.open(self.gcs_path, 'r') as gcs_file:
            return gcs_file.read()
