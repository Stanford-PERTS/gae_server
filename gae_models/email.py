"""
Email Model
===========

Model for sending queued emails
Uses 'mandrill.py' for sending
"""

from google.appengine.api import mail as app_engine_mail  # for development env
from google.appengine.ext import ndb
import datetime
import logging
import jinja2
import json

import config
import mandrill
import util

from .datastore_model import DatastoreModel


class Email(DatastoreModel):
    """An email in a queue (not necessarily one that has been sent).

    Emails have regular fields, to, from, subject, body. Also a send date and
    an sent boolean to support queuing.

    Body text comes in two flavors: template and raw html. Templates are
    served from /template/emails/*.html and should only use the filename
    i.e. "*.html"

    Sending of emails is done via Mandrill in mandrill.py
    """

    to_address = ndb.StringProperty(required=True)
    cc_address = ndb.StringProperty(repeated=True)
    bcc_address = ndb.StringProperty(repeated=True)
    from_address = ndb.StringProperty(default=config.from_server_email_address)
    reply_to = ndb.StringProperty(default=config.from_server_email_address)
    from_name = ndb.StringProperty(default="PERTS")
    subject = ndb.StringProperty(default="A message from PERTS")
    body = ndb.TextProperty()
    html = ndb.TextProperty()
    # Templates use jinja syntax for interpolation. They are processed locally
    # into text and html bodies, which are sent to mandrill via the api.
    template = ndb.StringProperty()
    template_data_json = ndb.TextProperty(default=r'{}')
    # These templates are different. They reference templates created in our
    # mandrill account. Only the template name goes to the mandrill api, and
    # it does the rest.
    mandrill_template = ndb.StringProperty()
    mandrill_template_content_json = ndb.TextProperty(default=r'{}')
    scheduled_date = ndb.DateProperty(auto_now_add=True)
    was_sent = ndb.BooleanProperty(default=False)
    was_attempted = ndb.BooleanProperty(default=False)
    errors = ndb.TextProperty()

    json_props = ['template_data_json', 'mandrill_template_content_json']

    @property
    def template_data(self):
        return (json.loads(self.template_data_json)
                if self.template_data_json else None)

    @template_data.setter
    def template_data(self, obj):
        self.template_data_json = json.dumps(obj)
        return obj

    @property
    def mandrill_template_content(self):
        return (json.loads(self.mandrill_template_content_json)
                if self.mandrill_template_content_json else None)

    @mandrill_template_content.setter
    def mandrill_template_content(self, obj):
        self.mandrill_template_content_json = json.dumps(obj)
        return obj

    @classmethod
    def we_are_spamming(self, email):
        """Did we recently send email to this recipient?"""

        to = email.to_address

        # We can spam admins, like pert_dev@googlegroups.com
        # so we white list them in the config file
        if to in config.addresses_we_can_spam:
            return False

        # We can also spam admins living at a @perts.net
        if to.endswith('perts.net'):
            return False

        since = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=config.suggested_delay_between_emails)

        query = Email.query(Email.was_sent == True,
                            Email.scheduled_date >= since,
                            Email.to_address == to,)

        return query.count(limit=1) > 0

    @classmethod
    def send(self, emails):
        """Send emails w/o spamming.
        Called as part of /cron/send_pending_email.
        """
        to_addresses = []
        for email in emails:

            ## @todo(chris): for now, we want people to get notifications,
            ## even if they generate/receive them quickly. Ignore spamming
            ## and multiple-addressing for now. Later, we may want to delay
            ## these, then digest them over the course of X time.
            # if self.we_are_spamming(email):
            #     # Do not send
            #     # This user has already recieved very recent emails

            #     # Debugging info
            #     logging.error("We are spamming {}:\n{}"
            #                   .format(email.to_address, email.to_dict()))
            #     # Note that we attempted to send so that we don't keep trying.
            #     email.was_attempted = True
            #     email.put()
            # elif email.to_address in to_addresses:
            #     # Do not send
            #     # We don't send multiple emails to an address per 'send'

            #     # Debugging info
            #     logging.error("We are spamming {}:\n{}"
            #                   .format(email.to_address, email.to_dict()))
            #     # Note that we attempted to send so that we don't keep trying.
            #     email.was_attempted = True
            #     email.put()
            if False:
                pass  # @todo(chris) put spamming logic back in here.
            else:
                # Not spam! Let's send it
                to_addresses.append(email.to_address)
                email.deliver()

    @classmethod
    def fetch_pending_emails(self, n=10):
        """Defaults to limiting sending 10 per cron job, which is every min."""
        to_send = Email.query(
            Email.deleted == False,
            Email.scheduled_date <= datetime.datetime.utcnow(),
            Email.was_sent == False,
            Email.was_attempted == False,
        )

        return to_send.fetch(n)

    @classmethod
    def send_pending_email(self):
        """Send the next unsent emails in the queue.
        Called as part of /cron/send_pending_email.
        """
        emails = self.fetch_pending_emails()

        if emails:
            self.send(emails)
            return emails
        else:
            return None

    def deliver(self):
        """Method to directly deliver email using Mandrill."""
        logging.info(u"to: {}".format(self.to_address))
        logging.info(u"subject: {}".format(self.subject))

        kwargs = {
            'to_address': self.to_address,
            'subject': self.subject,
            'template_data': self.template_data,
        }

        # Send certain entity properties as mandril api kwargs, if they're
        # set.
        attr_kwargs = ('body', 'html', 'template', 'mandrill_template',
                       'mandrill_template_content', 'from_address',
                       'reply_to', 'from_name', 'cc_address', 'bcc_address')
        for attr in attr_kwargs:
            v = getattr(self, attr)
            if v:
                kwargs[attr] = v

        result = mandrill_send(**kwargs)

        self.was_attempted = True
        self.was_sent = bool(result)  # result is None if send failed
        self.put()


# Setup jinja2 environment using the email subdirectory in templates
JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader('templates/emails'),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


def mandrill_send(template_data={}, **kwargs):

    # Determine if message should send
    if util.is_development() and not config.should_deliver_smtp_dev:
        logging.info('Email not sent, check config!')
        return None

    subject = render(kwargs['subject'], **template_data)

    # Add in default template data
    template_data['to_address'] = kwargs.get('to_address', None)
    template_data['domain'] = util.get_domain()
    # Python keeps time to the microsecond, but we don't need it, and
    # it's easier to render as ISO 8601 without it.
    template_data['server_time'] = datetime.datetime.today().replace(microsecond=0)
    template_data['contact_email_address'] = config.from_server_email_address

    # Determine if using html string or a template
    html_body = None
    if 'html' in kwargs:
        html_body = kwargs['html']
    elif 'body' in kwargs:
        html_body = render(kwargs['body'], **template_data)
    elif 'template' in kwargs:
        html_body = render_template(kwargs['template'], **template_data)

    text_body = kwargs.get('text', None)

    if util.is_localhost() or util.is_testing():
        sender = _send_localhost_and_testing
    elif util.is_development():
        sender = _send_development
    else:
        sender = _send_production

    optional_mandrill_keys = ('from_address', 'reply_to', 'from_name')
    optional_mandrill_kwargs = {k: kwargs[k] for k in optional_mandrill_keys
                                if k in kwargs}

    return sender(kwargs['to_address'], subject, html_body, text_body,
                  kwargs.get('mandrill_template', None),
                  kwargs.get('mandrill_template_content', None),
                  kwargs.get('cc_address', None),
                  kwargs.get('bcc_address', None),
                  **optional_mandrill_kwargs)


def _send_localhost_and_testing(to_address, subject, html_body=None,
                                text_body=None, mandrill_template=None,
                                mandrill_template_content=None,
                                cc_address=None, bcc_address=None, **kwargs):
    """Print the email in the logs so we can see it."""
    logging.info(html_body)
    logging.info(text_body)
    logging.info(mandrill_template)
    logging.info(mandrill_template_content)
    logging.info(kwargs)

    # We often have to test inviting people on our localhosts. Make this
    # easier, esp. re: different ports for webpack and webserver.
    link = (
        mandrill_template_content.get('link', None)
        if mandrill_template_content else None
    )
    if link:
        link = link.replace('10080', '3000')
        link = link.replace('8080', '8888')
    logging.info("""

vvv Invitation/Reset Link vvv

{}

^^^^^^^^^^^^^^^^^^^^^^^

""".format(link))

    return True


def _send_development(to_address, subject, html_body=None, text_body=None,
                      mandrill_template=None, mandrill_template_content=None,
                      cc_address=None, bcc_address=None, **kwargs):
    """Like production, use mandrill.  See commented code for sending via
    App Engine, which has become a bad idea b/c quotas are quite low."""

    # # Deprecated app engine sending code. Retained for reference should we
    # # want to switch back quickly.
    # return app_engine_mail.send_mail(
    #     config.from_server_email_address,
    #     to_address,
    #     subject,
    #     text_body or '',
    #     reply_to=config.from_server_email_address,
    #     html=html_body,
    # )

    return _send_production(to_address, subject, html_body, text_body,
                            mandrill_template, mandrill_template_content,
                            cc_address, bcc_address, **kwargs)


def _send_production(to_address, subject, html_body=None, text_body=None,
                     mandrill_template=None, mandrill_template_content=None,
                     cc_address=None, bcc_address=None, **kwargs):
    """Use Mandrill. Requires the SecretValue 'mandrill_api_key' be set."""

    # Most simple messages use the "send" api.
    # https://www.mandrillapp.com/api/docs/messages.JSON.html#method=send
    url = "messages/send.json"

    # JSON for Mandrill HTTP POST request
    json_mandrill = {
        "message": {
            # If this email is based on Mandrill templates, it will need data
            # to interpolate. We use handlebars {{ like this }}, b/c it's more
            # flexible and can put data, e.g. in an href attribute. That means
            # using the "global_merge_vars" part of the API. If using
            # "template_content" then templates must use interpolation points
            # like this: <span mc:edit="field_name">default text</span> which
            # is less flexible.
            "merge_language": "handlebars",
            "global_merge_vars": [],  # to be filled in, maybe
            "html": html_body,
            "text": text_body,
            "subject": subject,
            "from_email": kwargs.get('from_address', config.from_server_email_address),
            "from_name": kwargs.get('from_name', config.from_server_name),
            "inline_css": True,
            "to": format_to_addresses(to_address, cc_address, bcc_address),
        }
    }

    if 'reply_to' in kwargs:
        json_mandrill['message']['headers'] = {'Reply-To': kwargs['reply_to']}

    if mandrill_template:
        # These messages uses mandrill-managed templates and use a different
        # subset of the api.
        url = "messages/send-template.json"

        json_mandrill['template_name'] = mandrill_template
        json_mandrill['template_content'] = []

        # Wrangle content from basic key-value dictionary into what the api
        # expects.
        content_vars = [
            {'name': k, 'content': v}
            for k, v in mandrill_template_content.items()
        ]
        json_mandrill['message']['global_merge_vars'] = content_vars

        # Don't use the default subject, it should be set in the template.
        del json_mandrill['message']['subject']

    logging.info(json.dumps(json_mandrill))

    # URL for Mandrill HTTP POST request
    return mandrill.call(url, json_mandrill)


def format_to_addresses(to, cc=None, bcc=None):
    """Formats the "to" field for Mandrill API

    Args:
        to - string of one email address or tuple/list of several
        cc - string of one email address or tuple/list of several
        bcc - string of one email address or tuple/list of several

    Returns: "to" value per docs: https://www.mandrillapp.com/api/docs/messages.JSON.html#method=send
    """
    def f(raw, typ):
        addresses = [raw] if isinstance(raw, basestring) else raw
        return [{'email': email, 'type': typ} for email in addresses]

    return f(to, 'to') + f(cc or [], 'cc') + f(bcc or [], 'bcc')


def render(s, **template_data):
    """Creates email html from a string using jinja2."""
    # This function has been generating weird cascading error emails, full of
    # nested text where the escaping backslashes compound recursively. I'm not
    # sure how to fix it, but I'm hoping this will untangle the knot a bit.
    try:
        rendered = jinja2.Environment().from_string(s).render(**template_data)
    except jinja2.TemplateSyntaxError:
        rendered = u"""
Could not render email. The following are the original template
string and data.

{}

{}
""".format(s, template_data)

    return rendered


# Loads email html from a template using jinja2
def render_template(template, **template_data):
    return JINJA_ENVIRONMENT.get_template(template).render(**template_data)
