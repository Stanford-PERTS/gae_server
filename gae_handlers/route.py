from webapp2_extras.routes import RedirectRoute


class Route(RedirectRoute):
    """Webapp route subclass that handles trailing slashes gracefully.

    https://webapp-improved.appspot.com/api/webapp2_extras/routes.html
    """
    def __init__(self, template, handler, strict_slash=True, name=None,
                 **kwargs):

        # Routes with 'strict_slash=True' must have a name
        if strict_slash and name is None:
            # Set a name from the template
            # ** Be sure this isn't creating duplicate errors
            # ** but 'template' should be unique so I think it's good.
            name = template

        return super(Route, self).__init__(
            template, handler=handler, strict_slash=strict_slash, name=name,
            **kwargs
        )
