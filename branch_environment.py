#!/usr/bin/env python

"""Process the branch_environment.json file to create other config files
(e.g. app.yaml) from templates as appropriate for the current branch."""


from string import Template
import copy
import json
import os
import re
# preferred over pyyaml b/c it preserves comments,
# see http://stackoverflow.com/questions/7255885/save-dump-a-yaml-file-with-comments-in-pyyaml
import ruamel.yaml


def interp(available_values, template):
    """Do a regex sub of values into the template."""
    if not isinstance(template, basestring):
        return template
    return Template(template).safe_substitute(available_values)


def recursive_map(f, d):
    """Map f over d, including stepping into any nested dictionaries."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = recursive_map(f, v)
        else:
            out[k] = f(k, v)
    return out


def get_branch():
    # The CI_BRANCH environment variable should be set if we are in a
    # codeship build environment.
    branch = os.environ.get('CI_BRANCH', None)
    if not branch:
        # Otherwise, try to get it from local git.
        stream = os.popen("git rev-parse --abbrev-ref HEAD")
        branch = stream.read().strip()
        stream.close()
    if not branch:
        # If it's still not defined, stop to debug.
        raise Exception("Branch could not be determined.")
    return branch


def load_branch_conf(branch):
    with open('branch_environment.json', 'r') as file_handle:
        branch_environments = json.loads(file_handle.read())

    # Look up the part of the configuration for this branch. If this
    # branch isn't in the conf, default to dev.
    if branch.startswith('dev-'):
        conf_key = 'dev-*'
    elif branch not in branch_environments:
        conf_key = 'dev'
    else:
        conf_key = branch

    conf = branch_environments[conf_key]

    # The app.yaml part of the environment config may have interpolations to
    # process.
    conf['app.yaml'] = recursive_map(
        lambda k, v: interp({'branch': branch}, v),
        conf['app.yaml'],
    )

    return conf


def main():
    branch = get_branch()
    conf = load_branch_conf(branch)
    app_conf = conf['app.yaml']

    with open('app.template.yaml', 'r') as file_handle:
        # Read as a dictionary.
        app_yaml = ruamel.yaml.load(file_handle.read(),
                                    ruamel.yaml.RoundTripLoader)

    # These parts of app.yaml use syntax that could collide with our
    # interpolation syntax. We never want to replace values here.
    no_replace = ('handlers', 'skip_files')
    # Step through every value, even nested ones, that might have an
    # interpolation point, and replace it with the matching value from conf.
    replaced = recursive_map(
        lambda k, v: interp(app_conf, v),
        {k: v for k, v in app_yaml.items() if k not in no_replace}
    )
    app_yaml.update(replaced)

    # Write an app.yaml file that deployed App Engine can use.
    with open('app.yaml', 'w') as file_handle:
        file_handle.write(ruamel.yaml.dump(
            app_yaml, Dumper=ruamel.yaml.RoundTripDumper))

    # Write a separate app.yaml that codeship can use for running
    # dev_appserver.py (useful for e2e testing), which includes codeship
    # environment variables.
    is_codeship = os.environ.get('CI', None) == 'true'
    if is_codeship:
        # Read in standard config.
        with open('app.yaml', 'r') as file_handle:
            # Read as a dictionary since we DO care about semantics.
            # Don't use the RoundTripLoader and *Dumper b/c we don't need the
            # comments.
            app_yaml = ruamel.yaml.load(file_handle.read(),
                                        ruamel.yaml.RoundTripLoader)

        # Modify the config for codeship, and write a specific file for the
        # sdk to use.
        env = app_yaml.get('env_variables', {})
        env['CI'] = os.environ['CI']
        env['MYSQL_USER'] = os.environ['MYSQL_USER']
        env['MYSQL_PASSWORD'] = os.environ['MYSQL_PASSWORD']
        app_yaml['env_variables'] = env
        with open('app.codeship.yaml', 'w') as file_handle:
            yaml_str = ("# For Codeship Protractor tests. Adds environment\n"
                        "# variables related to MySQL.\n")
            yaml_str += ruamel.yaml.dump(app_yaml,
                                         Dumper=ruamel.yaml.RoundTripDumper)
            file_handle.write(yaml_str)

        # Also write environment information to disk so we can customize the
        # deploy script. See codeship_deploy.sh in your project.
        with open('project_id.txt', 'w') as file_handle:
            file_handle.write(env['PROJECT_ID'])

        with open('app_engine_version.txt', 'w') as file_handle:
            file_handle.write(env['APP_ENGINE_VERSION'])

    # Process the cron.yaml part of the environment config.
    cron_conf = conf['cron.yaml']

    with open('cron.template.yaml', 'r') as file_handle:
        # Read as a dictionary since we DO care about semantics.
        cron_yaml = ruamel.yaml.load(file_handle.read(),
                                     ruamel.yaml.RoundTripLoader)

    # Determine which of the available cron jobs to use.
    default = cron_conf.get('_default', True)
    enabled = lambda cron_job: cron_conf.get(cron_job['url'], default)
    enabled_jobs = [job for job in cron_yaml['cron'] if enabled(job)]

    # Interpolate data into cron job settings from the app part of the conf
    # and overwrite existing jobs with those we've chosen.
    cron_yaml['cron'] = [recursive_map(lambda k, v: interp(app_conf, v), job)
                         for job in enabled_jobs]

    # Write an cron.yaml file that App Engine can use.
    with open('cron.yaml', 'w') as file_handle:
        file_handle.write(ruamel.yaml.dump(
            cron_yaml, Dumper=ruamel.yaml.RoundTripDumper))


if __name__ == "__main__":
    print "branch_environment.py building various config files..."
    main()
    print "...success"
