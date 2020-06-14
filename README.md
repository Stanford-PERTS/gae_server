# GAE Server

Python server code for Google App Engine, to be shared among all modern PERTS applications. To be included as a submodule in other repositories. See github.com/PERTS/neptune and github.com/PERTS/triton.

## Git, codeship, and permissions

Being a private repository and submodule means that cloning this repo from within a Codeship build is non-trivial. We followed the related [Codeship documentation](https://documentation.codeship.com/basic/continuous-integration/git-submodules/#submodule-permissions) using a _machine user_ on github with the username `perts-machine` and email support@perts.net.

The public SSH keys defined by the Neptune and Triton Codeship projects are associated with this machine user (not as Deploy Keys on the repos).
