# Operations

## Create a repository

`motte repo create [--owner OWNER] NAME` creates a native repository and prints its path. The default owner is `operator`.

A control plane that claims workspaces on a repository sends an owner in each claim. The claim fails with `vcs_repo_forbidden` when the owner of the repository row is another one. So create a repository that a control plane uses with `--owner` set to the owner that the control plane sends, for example `motte repo create --owner owner_operator NAME`.
