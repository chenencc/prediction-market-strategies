# Engine upgrade workflow

## Branch ownership

In the public engine fork:

* `develop` is a clean mirror of official `upstream/develop`; never commit our
  strategies or engine patches on it.
* `codex/engine` contains only engine changes we need, initially zero patches.
* `codex/upgrade-YYYYMMDD` is a temporary integration branch used for an upgrade.

Our private strategy repository uses `main` for the accepted research baseline
and `codex/*` branches for implementation. Strategies, private parameters and
dataset access configuration remain here, outside the public fork.

## One upgrade

1. Fetch the official `upstream` remote in the engine checkout.
2. Fast-forward clean `develop` only (`git merge --ff-only upstream/develop`).
3. Create an upgrade branch from `codex/engine` and merge the reviewed upstream
   commit. Resolve conflicts without dropping our patches or force-syncing over
   them. Read API, fee, instrument and simulation changes even if Git has no conflict.
4. Build the engine and run its relevant regression suite, then our offline
   dataset/adapter/strategy tests against that exact build.
5. Compare the same frozen dataset, configuration and random seed: event counts,
   timestamps, instrument mappings, order/fill behavior, fees and results. Explain
   every material difference; preserve the old baseline for reproducibility.
6. Merge the accepted engine branch and change `engine.lock.json` in a private
   strategy PR. Record the exact commit and relevant patch/test receipts.
7. Activate the new dependency only after validation. An upstream push, successful
   merge or CI result alone does not authorize deployment or trading.

The strategy environment must resolve an exact commit/build, never the floating
`develop` or `codex/engine` branch. A version label alone does not distinguish
unreleased patches. Keep the engine source revision, build/runtime identity,
dataset revision and backtest configuration with each run.

## Remotes

The local engine checkout uses `origin` for our fork and `upstream` for the
official repository. The private strategy checkout has its own `origin` only.
Do not add the public engine remote as an upstream of the private strategy repo.

GitHub fork synchronization and merge conflicts are described in:
https://docs.github.com/en/pull-requests/how-tos/work-with-forks/syncing-a-fork

The engine's upstream license and notices remain with the engine. Do not copy
engine code into this private project to bypass its license obligations.
