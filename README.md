# AcademicClusters

[![Build Status](https://github.com/brendanjohnharris/AcademicClusters.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/brendanjohnharris/AcademicClusters.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/brendanjohnharris/AcademicClusters.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/brendanjohnharris/AcademicClusters.jl)

A Julia package for running distributed work on academic compute clusters. It provides a `ClusterManager` that launches `Distributed.jl` workers, helpers for submitting scripts as batch (and array) jobs, and a scheduler that spreads workers across queues and shared machines according to live free capacity.

Currently there is one site module, `USydPhysics`, which targets the University of Sydney School of Physics PBS Pro cluster and its shared lab machines.

## Installation

```julia
using Pkg
Pkg.add(url = "https://github.com/brendanjohnharris/AcademicClusters.jl")
```

## Requirements

The package shells out to `ssh` and `qsub` rather than linking against a PBS library, so a few environmental things must hold:

- **`ssh headnode` must work non-interactively** from wherever you run Julia, and `ssh <hpc>` likewise for the shared machines. Define these as `Host` aliases in `~/.ssh/config` with key-based auth.
- **`~/.bashrc` must not print to stdout.** Job submission is parsed from the output of `ssh headnode "source ~/.bashrc && qsub ..."`; anything your shell profile echoes corrupts the job ID and raises an error saying so.
- **The log directory must be on shared storage.** `qsub` runs on the headnode and tasks run on compute nodes; neither sees the submit host's node-local `/tmp`. Workers publish their host and port by writing to their log file, so the main process must be able to read it.
- **A shared filesystem for the Julia binary**, for `distributeprocs` only: workers on shared machines are launched with `joinpath(Sys.BINDIR, "julia")`, assuming the same path resolves everywhere.

## Configuration

There is one setting, the log directory, defaulting to `~/.jobs`. It holds the generated PBS scripts, the worker handshake logs, and job stdout. Set it with a preference (recommended, since it is baked in at precompile time):

```julia
using Preferences, AcademicClusters
set_preferences!(AcademicClusters, "logdir" => "/import/taiji1/user/.jobs")
```

or with the environment variable `AcademicClusters_LOGDIR`. Resolution order is preference, then environment, then default.

## Usage

The API lives in the `USydPhysics` submodule; `AcademicClusters` itself exports nothing.

```julia
using AcademicClusters.USydPhysics
```

Note that `USydPhysics.addprocs` is a separate function from `Distributed.addprocs`, not a method of it. Loading both modules unqualified makes the bare name ambiguous and Julia will refuse to resolve it; either use `USydPhysics` alone (as above) and call `Distributed.addprocs` qualified, or import `USydPhysics` qualified and call `USydPhysics.addprocs`.

### Interactive workers

`addprocs` submits a PBS job (an array job with one task per worker when `np > 1`) and blocks until the workers connect:

```julia
procs = addprocs(4; ncpus = 8, mem = 16, walltime = 24, queue = `taiji`)

@everywhere f() = myid()
remotecall_fetch(f, first(procs))

rmprocs(procs)  # workers exit, ending their jobs; ssh tunnels are torn down
```

| Argument | Default | Meaning |
|---|---|---|
| `np` | | Number of workers |
| `ncpus` | `8` | Cores per worker |
| `ngpus` | `0` | GPUs per worker |
| `mem` | `16` | Memory per worker |
| `walltime` | `24` | Maximum runtime |
| `queue` | `` `` `` | PBS queue, as a `Cmd` |
| `project` | `` `` `` | Project directory; defaults to the active project |
| `qsubflags` | `` `` `` | Extra flags passed to `qsub` |

Remaining keyword arguments are forwarded to `Distributed.addprocs`. The underlying `PBSProManager` takes the same arguments and can be passed to `Distributed.addprocs` directly if you want to bypass the wrapper.

Workers are given `-t auto` and a heap-size hint of half their requested memory.

### Distributing across queues and shared machines

`distributeprocs` probes the cluster and the shared machines for free capacity, then splits `np` workers between them. Cluster workers go through PBS with enforced resource limits; workers on shared machines are launched over ssh with `-t ncpus` threads and a heap hint, but no scheduler enforcement, so they are throttled politely by `saturation`.

```julia
procs = distributeprocs(20; ncpus = 2, mem = 8, walltime = 12)
procs = distributeprocs(10; hpcratio = 0.9)  # bias toward the shared machines
procs = distributeprocs(Inf; ncpus = 2)      # fill 90% of everything available
```

| Argument | Default | Meaning |
|---|---|---|
| `np` | | Total workers; `Inf` fills all available capacity, less `buffer` |
| `buffer` | `0.1` | With `np = Inf`, fraction of each pool left free |
| `ncpus` | `1` | Cores per worker |
| `mem` | `4` | Memory per worker |
| `walltime` | `24` | Walltime, for cluster jobs only |
| `hpcratio` | `0.5` | 0 = all cluster, 1 = all shared machines, 0.5 = follow free capacity |
| `saturation` | `0.75` | Maximum fraction of a shared machine's cores and available memory to occupy |
| `queues` | `["defaultQ", "taiji"]` | PBS queues to draw on |
| `hpcs` | `["orr", "cartman", "karl"]` | ssh-reachable shared machines |

Capacity is measured per worker shape. For a queue it is the number of `ncpus` by `mem` workers that fit in the free cores and memory of vnodes serving that queue, capped by the per-user running-ncpus limit; routing queues are followed one hop to their execution queue. For a shared machine it is derived from `nproc`, `/proc/loadavg`, and available memory, scaled by `saturation` and reduced by the workers this session already placed there (load average lags freshly spawned workers, so repeated calls would otherwise double-book). Pools are weighted by capacity times ratio, with overflow spilling to the other pool. If the total falls short, `distributeprocs` launches what fits and warns.

### Batch jobs

`runscript` submits a Julia script or expression as a PBS job and returns `(jobid, logfile)` without waiting:

```julia
jobid, logfile = runscript("myscript.jl"; ncpus = 10, mem = 64, walltime = 12)
jobid, logfile = runscript("myscript.jl"; args = ["input.csv", "out.jld2"])

jobid, logfile = runscript(quote
    using MyPackage
    MyPackage.go()
end)
```

An `Expr` is written out one top-level statement per line to a file in the log directory, then submitted like any other script.

`runscripts` submits many at once as a PBS array job, returning the job ID:

```julia
exprs = [:(compute($i)) for i in 1:100]
jobid = runscripts(exprs; ncpus = 4, mem = 16, walltime = 6)

jobid = runscripts("/path/to/scripts")  # a directory of 1.jl, 2.jl, ...
```

For the directory form, only files named `<integer>.jl` are counted, so stray files are ignored; the array is sized to that count.

Defaults for both are `ncpus = 10`, `mem = 31`, `walltime = 48`. `project`, `queue`, `qsubflags`, and `exeflags` are all accepted as `Cmd`s.

`selfdestruct()` qdels the current job from inside it, for a job that has decided it is done.

## Resource specifications

Memory takes a number (interpreted as GB) or a string with explicit units. Integer GB stay in GB; fractional values are converted to MB. Strings are validated against `^\d+(\.\d+)?[KMGT]B$` and passed through with their units intact.

```julia
mem = 16        # "16GB"
mem = 0.5       # "512MB"
mem = 1.5       # "1536MB"
mem = "2048MB"  # "2048MB"
```

Walltime takes a number of hours or an `HH:MM:SS` string. Hours are not capped at two digits, so week-long jobs are expressible either way.

```julia
walltime = 24          # "24:00:00"
walltime = 168         # "168:00:00"
walltime = "00:45:30"  # "00:45:30"
```

## How it works

Submission writes a generated `#PBS` script into the log directory and pipes it through `ssh headnode "source ~/.bashrc && /opt/pbs/bin/qsub ..."`. The job ID is parsed from the `JOBID.server` output, which is why a chatty `.bashrc` breaks things.

For `addprocs`, each worker starts as `julia --worker=<cookie>` with its stdout tee'd to a known log file. The main process polls for that file (backing off up to 5 seconds) and reads the host and port the worker prints. `JULIA_WORKER_TIMEOUT` bounds the wait, defaulting to 480 seconds; raise it if the queue is slow to start jobs.

How the main process then connects depends on where it is running. It checks for `/opt/pbs` locally: if present it is on the cluster and connects to the worker directly; if absent it opens an ssh tunnel through `headnode` to the worker, retrying on a random local port up to ten times. Tunnels are opened with `ControlPath=none` and `ControlMaster=no`, deliberately: these are many concurrent forward-only sessions, and sharing a `ControlMaster` makes them race and die. Tunnels are tracked and killed with their workers, and cleaned up if a launch fails partway.

Logs land in the log directory. Single jobs write `<jobid>.<id>.log`; array jobs write a directory `<jobid>[].<id>.log/` containing one `<index>.log` per task. PBS's own final output for a job goes to `<id>.final.log`.

## Tests

```julia
using Pkg
Pkg.test("AcademicClusters")
```

The suite splits in two. The unit tests cover resource parsing, capacity probing, and worker allocation against recorded fixtures of real `pbsnodes` and `qstat` output, and need no cluster. The integration tests submit real jobs and only make sense on a `physics.usyd.edu.au` host. `runtests.jl` gates both behind that host check.
