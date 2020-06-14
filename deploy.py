#!/usr/bin/env python

"""Deploy this project directly to App Engine from the local filesystem.

## DANGER!!

This will put your files, exactly as they exist on your hard drive, into a
deployed environment. BE SURE you understand how ALL the following will
influence the result:

* git status: clean? working or untracked changes?
* git commit: HEAD? a rollback?
* git branch: being in certain branches control where the deploy goes
* project: what directory are you in? are you deploying the project/website you
  intend?

## Emergency Rollback

You screwed up a deploy and don't want to fight with codeship to get an old/working
commit out.

1. Checkout a working commit. Can be a brach HEAD or any other commit.
2. Test it locally to make sure it works the way you think it does.
3. Run this script from the command line: `./deploy.py`
4. Check the output is right re: project and version.
5. Run this script again with the force option: `./deploy.py --force`
  - All tests will run. If any fail, it won't deploy.
  - A production build of the client code will run.
  - You'll have a final chance to confirm before files are uploaded.
"""

import optparse
import os
import ruamel.yaml
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'gae_server'))

import branch_environment


def cmd(cmd_str):
    process = subprocess.Popen(cmd_str.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        raise Exception("Bash process non-zero exit value: {}".format(error))
    return output


def git_is_clean():
    is_clean = cmd('git status --porcelain') == ''
    if not is_clean:
        print "Your git isn't clean. Run `git status`, clean up, and try again."
    return is_clean


parser = optparse.OptionParser()
parser.add_option("-f", "--force", action="store_true", dest="force",
                  default=False,
                  help="Actually deploy, rather than preview settings.")
(options, args) = parser.parse_args()

# Process all the config files for this environment.
branch_environment.main()

with open('app.yaml', 'r') as fh:
    app_yaml = ruamel.yaml.load(fh.read(), ruamel.yaml.RoundTripLoader)

project_id = app_yaml['env_variables']['PROJECT_ID']
version = app_yaml['env_variables']['APP_ENGINE_VERSION']

if options.force:
    if not git_is_clean():
        sys.exit(1)
    print "Running a full build locally. This will take a minute or two."
    cmd('./codeship_setup.sh')
    cmd('./codeship_test.sh')
    # Note: the no-promote option prevents App Engine from switching traffic
    # to the deployed version. Because we have specific versions dedicated to
    # production, we never want this, so always use no-promote.
    cmd("gcloud app deploy app.yaml --project={project} "
        "--version={version} --no-promote"
        .format(project=project_id, version=version))
else:
    if not git_is_clean():
        sys.exit(1)

    print (
        "Would deploy to `{project}`, version `{version}`. If correct, run "
        "again with --force."
        .format(project=project_id, version=version)
    )
