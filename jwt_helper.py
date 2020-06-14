"""jwt_helper

Standarizes JWT options for communicating between PERTS servers. Currently this
is only Neptune and Triton, but the allowed remote server uuid environment
variable(s) could be configured to allow any number.
"""

# JWTs are signed so you know 1) only the holder of the secret created the data
# and 2) the data hasn't changed. However, JWT payloads are not encrypted in
# themselves, so their security is dependent on the transmission protocol (e.g.
# https).
#
# https://pypi.python.org/pypi/PyJWT/1.4.0
# https://stormpath.com/blog/jwt-the-right-way

from google.appengine.api import memcache
import datetime
import logging
import time
import uuid

from jwt import InvalidTokenError, DecodeError
from jwt.exceptions import InvalidKeyError
import jwt

from model import SecretValue
import config
import util

from jwt.contrib.algorithms.pycrypto import RSAAlgorithm

# jwt.unregister_algorithm('RS512')
jwt.register_algorithm('RS512', RSAAlgorithm(RSAAlgorithm.SHA512))


ALGORITHM = 'HS512'
ALGORITHM_RSA = 'RS512'


EXPIRED = 'expired'
NO_USER = 'no user'
NOT_FOUND = 'not found'
USED = 'used'


def get_secret():
    """Get secret for signing/verifying symmetric token. Defaults to config."""
    sv_entity = SecretValue.get_by_id('jwt_secret')
    if sv_entity is None:
        if not util.is_development():
            logging.error("No default jwt secret set in production!")
        return config.default_jwt_secret
    else:
        return sv_entity.value


def get_secret_rsa():
    """Get secret key for signing asymmetric token. Defaults to config."""
    sv_entity = SecretValue.get_by_id('jwt_secret_rsa')
    if sv_entity is None:
        if not util.is_development():
            logging.error("No default jwt rsa secret key set in production!")
        return config.default_jwt_secret_rsa
    else:
        return sv_entity.value


def get_public_rsa():
    """Get key for verifying asymmetric token. Defaults to config."""
    sv_entity = SecretValue.get_by_id('jwt_public_rsa')
    if sv_entity is None:
        if not util.is_development():
            logging.error("No default jwt rsa public key set in production!")
        return config.default_jwt_public_rsa
    else:
        return sv_entity.value


def get_payload(user):
    return {'user_id': user.uid, 'email': user.email}


def _decode(token, secret, algorithm, validate_jti=False, cache_jti=True):
    """Decode and verify a received token into a data payload.

    Args:
        token - str, base-64 encoded json web token string
        secret - str, key for signature verification (in the case of asymm
            signatures, this is the public key, so it's not really "secret")
        algorithm - str, currently either 'HS512' or 'RS512', see
            https://github.com/jpadilla/pyjwt/blob/master/docs/algorithms.rst
            for all option supported.
        validate_jti - bool, default False, whether the server should check if
            it has seen this jti before. Set to True for endpoints that need
            extra security, like /api/set_password.
        cache_jti - bool, default True, only relevant if validate_jti is True.
            Whether the server should cache the jti claim when validating it to
            ensure this token is recognized if it is used again. Should never
            permit the client to set this to False. Set to True for pre-
            screening tokens on the set_password page.

    When to use which?
    * For normal data calls, use the defaults so users can re-use tokens and
      run calls in parallel with the same token.
    * For pre-checking a token upon entry to the set password page, use
      validate_jti=True and cache_jti=False so you can get the validity of the
      token and still use it a second time to set the password.
    * For setting a password, use validate_jti=True and cache_jti=True so the
      token is cached/consumed and no one else can steal the link to change the
      user's password.

    Returns a tuple as (payload dict or None, error str or None) where the
    error may be 'not found', 'used', or 'expired', just like
    AuthToken.checkTokenString.
    """
    if not token:
        # Token is None or empty string. Most likely scenario is the user isn't
        # supposed to be signed at in all (e.g. at / or /login), so don't log
        # an error otherwise put up a fuss.
        return (None, NOT_FOUND)

    options = {
        'require_exp': True,
        # Although we don't always validate the jti claim against the cache of
        # seen tokens, we do always require the claims exists.
        'require_jti': True,
        'require_iat': True,
    }
    payload = None
    error = None
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithm=algorithm,
            options=options,
        )
    except ValueError as e:
        # This may happen if the wrong algorithm is used, like using
        # jwt_helper.decode(rsa_token), because that decoder is based on HS512.
        logging.info("jwt_helper._decode caught ValueError:")
        logging.info(e)
        error = NOT_FOUND
    except DecodeError:
        # This is a more serious problem, it means we're not signing our jwts
        # correctly or someone is hacking us.
        logging.info("Signed with invalid secret.")
        logging.info(token)
        error = NOT_FOUND
    except InvalidKeyError:
        # This might happen if we attempt to decode a symmetric jwt with an
        # asymmetric key, or vice versa.
        logging.info("InvalidKeyError: tried to decode with a key that "
                     "doesn't match the algorithm.")
        logging.info(token)
        error = NOT_FOUND
    except InvalidTokenError:
        # This is normal any time someone's session expires, so just info.
        logging.info("Something else wrong; bad expiration other claim.")
        logging.info(token)
        error = EXPIRED

    if payload and validate_jti and not valid_jti(payload, cache_jti):
        error = '{} {}'.format(USED, exp_to_string(payload['exp']))
        payload = None

    return (payload, error)


def decode(token, **kwargs):
    """Decode and verify a token, symmetric encryption, returns payload."""
    return _decode(token, get_secret(), ALGORITHM, **kwargs)


def decode_rsa(token, **kwargs):
    """Decode and verify a token, asymmetric encryption, returns payload."""
    return _decode(token, get_public_rsa(), ALGORITHM_RSA, **kwargs)


def valid_jti(payload, cache_jti=True):
    """Make sure the jti, uuid for this token, hasn't been used before."""
    # Structure is: {jti: expiration datetime, ...}
    known_jtis = memcache.get('jwt_jtis') or {}

    # Clear out old jtis.
    now = datetime.datetime.now()
    known_jtis = {jti: exp for jti, exp in known_jtis.items() if exp > now}

    # Check if we've seen this uuid before.
    if payload['jti'] in known_jtis:
        return False

    # Memcache the jti for the future.
    if cache_jti:
        known_jtis[payload['jti']] = exp_to_datetime(payload['exp'])
        memcache.set('jwt_jtis', known_jtis)

    return True


def _encode(payload, secret, algorithm, expiration_minutes=60):
    """Encode and sign a data payload into a json web token.

    Args:
        payload - dict
        secret - str, key for signing jwt

    Returns: a json web token as a string.
    """
    claims = {
        'jti': str(uuid.uuid4()),
        'exp': int(time.time()) + 60 * expiration_minutes,
        'iat': int(time.time()),
    }
    payload = dict(payload, **claims)
    return jwt.encode(
        payload,
        secret,
        algorithm=algorithm,
    )


def encode(payload, **kwargs):
    return _encode(payload, get_secret(), ALGORITHM, **kwargs)


def encode_rsa(payload, **kwargs):
    return _encode(payload, get_secret_rsa(), ALGORITHM_RSA, **kwargs)


def encode_user(user, **kwargs):
    return encode(get_payload(user), **kwargs)


def exp_to_datetime(exp):
    return datetime.datetime.fromtimestamp(exp)


def exp_to_string(exp):
    return util.datelike_to_iso_string(exp_to_datetime(exp))
