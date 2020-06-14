"""How Neptune interacts with Mandrill (transactional email service)."""

from google.appengine.api import urlfetch
# Use ndb here instead of SecretValue to avoid circular imports. Grumble.
from google.appengine.ext import ndb
import json
import logging

import config
import util


def call(api_path, payload):
    """Make a synchronous call to the Mandrill API.

    See https://mandrillapp.com/api/docs/

    Args:
        api_path: str, just the last two parts of the path, e.g.
            'messages/send.json'. Varies by what part of the API you're using.
        payload: dict, always omitting the api key, which is added here in
            this function.

    Returns: None or parsed JSON from Mandrill response.
    """

    sv_entity = ndb.Key('SecretValue', 'mandrill_api_key').get()
    if sv_entity:
        payload['key'] = sv_entity.value
    else:
        if not util.is_development():
            logging.error("No mandrill api key set in production!")
        payload['key'] = config.default_mandrill_api_key

    try:
        result = urlfetch.fetch(
            url='https://mandrillapp.com/api/1.0/{}'.format(api_path),
            payload=json.dumps(payload),
            method=urlfetch.POST,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            # Default deadline appears to be 5 seconds, based on the numerous
            # errors we're getting. Give the Mandrill API more time to respond.
            # https://cloud.google.com/appengine/docs/standard/python/refdocs/google.appengine.api.urlfetch#google.appengine.api.urlfetch.fetch
            deadline=60,  # seconds
        )
    except urlfetch.DownloadError:
        logging.error("Caught urlfetch.DownloadError. "
                      "Request timed out or failed.")
        content = None
    else:
        if not result or result.status_code != 200:
            logging.error("Non-200 response from Mandrill.")
            content = None
        else:
            content = json.loads(result.content)
        logging.info("urlfetch result: {} {}".format(
            result.status_code, result.content))

    return content
