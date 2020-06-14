"""Abstract class for api request handlers following REST conventions."""

from google.appengine.api import taskqueue
import json
import logging

from gae_models import DatastoreModel, SqlModel, reverse_order_str
from permission import owns
import util

from .api import ApiHandler


class RestHandler(ApiHandler):
    """Ancestor for all RESTful resources."""

    def query(self, override_permissions=False):
        """For GET /api/<resource>, returns a list.

        Limited to super admins by default.

        Request params:
            n: int how many results to return
            ancestor: uid of entity defining some entity group, forces a
                strongly consistent result set.
            order: str name of some entity property, reverse with prefix '-'.
                Note that "reverse native" ordering is also possible with a
                simple '-'.
            uid: str optionally repeated, forces a strongly consistent result
                set of only those objects. Can't be mixed with other params.
            cursor: url-safe cursor references a previously run query as a
                position in the index to begin searching.

        Args:
            override_permissions: (bool) default False, set to True to remove
                all permission restrictions. Inheriting handlers can set this
                flag and then enforce their own permissions.
        """
        user = self.get_current_user()
        if user.super_admin or override_permissions:
            # These are generic request parameters.
            param_types = {'n': int, 'ancestor': str, 'order': str,
                           'uid': list, 'cursor': 'cursor'}

            # These are the readable properties of the model.
            param_types.update(self.model.property_types())

            # Get params from the request.
            params = self.get_params(param_types)

            if 'uid' in params:
                # This is a special form of query that works like a batch
                # get of specific ids. No other params are applicable, so they
                # shouldn't be present.
                if len(params) > 1:
                    raise Exception("Can't mix uid parameter with any others.")
                results = self.model.get_by_id(params['uid'])
                self.write(results)
                return results

            if user.super_admin:
                # Super admins default to unlimited query results.
                params['n'] = params.get('n', float('inf'))

            # Make the query and return the results.
            results = self.model.get(**params)
            self.write(results)

            # If the results have paging cursors, put them in the headers.
            links = self.build_link_header(results, params.get('order', ''))
            if links:
                self.response.headers['Link'] = links
            return results
        else:
            self.error(403)

    def build_link_header(self, results, order):
        n_cur = getattr(results, 'next_cursor', None)
        p_cur = getattr(results, 'previous_cursor', None)
        l_cur = getattr(results, 'last_cursor', None)

        if not n_cur and not p_cur:
            # These results don't have cursors, so don't use a link header.
            return None

        first_url = util.set_query_parameters(
            self.request.path_qs, cursor=None)
        previous_url = util.set_query_parameters(
            self.request.path_qs, cursor=p_cur.urlsafe())
        next_url = util.set_query_parameters(
            self.request.path_qs, cursor=n_cur.urlsafe())
        if l_cur:
            # SQL models _can_ tell us a cursor for the last page.
            last_url = util.set_query_parameters(
                self.request.path_qs, cursor=l_cur.urlsafe())
        else:
            # The Datastore usually can't tell us a cursor for the last
            # page of an index. Cheat by taking the first page of a
            # reverse-ordered query.
            last_url = util.set_query_parameters(
                self.request.path_qs, cursor=None,
                order=reverse_order_str(order))
        links = [
            '<{}>;rel=self'.format(self.request.path_qs),
            '<{}>;rel=first'.format(first_url),
            '<{}>;rel=previous'.format(previous_url),
            '<{}>;rel=next'.format(next_url),
            '<{}>;rel=last'.format(last_url),
        ]
        return ','.join(links)

    def get(self, id=None, override_permissions=False):
        """Get a list of entities or a particular entity.

        Args:
            override_permissions: (bool) default False, set to True to remove
                permission restrictions from ONLY id-based gets. See query().
        """
        if self.allowed_by_jwt:
            logging.info(
                "RestHandler overriding normal permission b/c this endpoint "
                "is explicitly allowed by the jwt."
            )
            override_permissions = True

        if not id:
            # For /api/<collection>, returns a list.
            return self.query(override_permissions=override_permissions)

        # For /api/<collection>/<id>, returns an object, strongly consistent,
        # or 403 or 404.
        result = self.model.get_by_id(id)
        if not result:
            self.error(404)
            return

        ok = override_permissions or owns(self.get_current_user(), result)
        if not ok:
            self.error(403)
            return

        self.write(result)
        return result

    def post(self, override_permissions=False):
        """For POST /api/<resource>, to create entities.

        Limited to super admins by default. See query().
        """
        user = self.get_current_user()
        if user.super_admin or override_permissions:
            params = self.get_params(self.model.property_types())
            new_entity = self.model.create(**params)
            new_entity.put()

            p = user.get_owner_property(new_entity)
            if p is not None:
                p.append(new_entity.uid)
                user.put()

            self.write(new_entity)
            return new_entity
        else:
            self.error(403)

    def put(self, id=None, override_permissions=False):
        """To modify a specified entity.

        Must own entity by default. See query().
        """
        if id is None:
            # Somebody called PUT /api/<collection> which we don't support.
            self.error(405)
            self.response.headers['Allow'] = 'GET, HEAD, POST'
            return

        id = self.model.get_long_uid(id)
        # Checking override first may save db time.
        if override_permissions or owns(self.get_current_user(), id):
            params = self.get_params(self.model.property_types())
            entity = self.model.get_by_id(id)
            for k, v in params.items():
                setattr(entity, k, v)
            entity.put()
            self.write(entity)
            return entity
        else:
            self.error(403)

    def delete(self, id=None, override_permissions=False):
        """To delete a specified entity.

        Must own entity by default. See query().
        """
        if id is None:
            # Somebody called DELETE /api/<collection> which we don't support.
            self.error(405)
            self.response.headers['Allow'] = 'GET, HEAD, POST'
            return

        user = self.get_current_user()
        id = self.model.get_long_uid(id)
        # Checking override first may save db time.
        if override_permissions or owns(user, id):
            entity = self.model.get_by_id(id)

            if isinstance(entity, DatastoreModel):
                entity.deleted = True
                entity.put()
            elif isinstance(entity, SqlModel):
                self.model.delete_multi([entity])

            # @todo: this seems logical, but doesn't deal with other users who
            # also own this thing. Don't want to implement until there's a
            # general solution.
            # p = user.get_owner_property(entity)
            # if p:
            #     p.remove(entity.uid)
            #     user.put()

            self.http_no_content()
            return entity
        else:
            self.error(403)

    def patch(self):
        user = self.get_current_user()
        if user.user_type == 'public':
            return self.http_unauthorized()

        calls = self.process_json_body()

        if not user.super_admin:
            for call in calls:
                # platform defaults to config
                call['endpoint'] = self.get_endpoint_str(
                    method=call['method'], path=call['path'])

            # To maximize stability, these PATCH requests should be idempotent.
            # That means only PUT and DELETE in the body.
            if not all(c['method'] in ('PUT', 'DELETE') for c in calls):
                return self.http_bad_request("Only PUT or DELETE allowed.")

            # There must be a whitelisted endpoint in the jwt for each call.
            if not all(self.jwt_allows_endpoint(c['endpoint']) for c in calls):
                return self.http_forbidden("Allowed endpoints insufficient.")

            # Each call must be within the scope of the current request, e.g.
            # you can't send calls to /api/codes via a PATCH to /api/users.
            if not all(self.request.path in c['path'] for c in calls):
                return self.http_bad_request("A call's path doesn't match.")

        # Delegate each call to existing code.
        tasks = []
        for call in calls:
            tasks.append(taskqueue.add(
                # The docs appear to say push tasks may not set a method, but
                # this code works anyway.
                # https://cloud.google.com/appengine/docs/standard/python/refdocs/google.appengine.api.taskqueue#google.appengine.api.taskqueue.add
                method=call['method'],
                url=call['path'],
                payload=(json.dumps(call['body'])
                         if 'body' in call and len(call['body']) > 0
                         else None),
                headers={
                    'Content-Type': 'application/json; charset=utf-8',
                    'Authorization': self.request.headers['Authorization'],
                },
            ))

        self.write([
            {
                'method': t.method,
                'url': t.url,
                'body': t.payload,
                'task_name': t.name,
                'was_enqueued': t.was_enqueued,
            }
            for t in tasks
        ])
