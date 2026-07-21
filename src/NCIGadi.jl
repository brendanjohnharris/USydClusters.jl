module NCIGadi
import Distributed
import AcademicClusters: pref_or_env, build_julia_command, LOGDIR, to_string,
    parse_memory, parse_walltime, memory_string_to_gb, with_heap_hint,
    capture_jobid, write_exprs, combine_exprs, next_runscripts_id,
    default_project

export runscript, runscripts, selfdestruct, gadi_defaults, distributeprocs

# No qsub-launching ClusterManager here: Gadi compute nodes cannot be reached
# from outside for the Distributed handshake, so only batch submission is
# supported. Within a running job, `distributeprocs` spans the allocated nodes.

function gadi_project()
    project = pref_or_env("gadi_project")
    isnothing(project) && error(
        """
        NCI project code not set. Set it with
            using Preferences, AcademicClusters
            set_preferences!(AcademicClusters, "gadi_project" => "ab12")
        or the environment variable ACADEMICCLUSTERS_GADI_PROJECT.
        """
    )
    return project
end

# Default walltime = queue cap x this fraction; shorter requests backfill sooner
# and cost nothing extra (SUs charge actual runtime, not the request)
function walltime_fraction()
    frac = pref_or_env("gadi_maxwalltime_fraction", 1 / 4)
    if frac isa AbstractString # env form; accept "0.25" or "1/4"
        frac = if occursin('/', frac)
            n, d = parse.(Float64, split(frac, '/', limit = 2))
            n / d
        else
            parse(Float64, frac)
        end
    end
    0 < frac <= 1 ||
        throw(ArgumentError("gadi_maxwalltime_fraction must be in (0, 1], got $frac"))
    return frac
end

default_walltime(cap) = max(1, floor(Int, cap * walltime_fraction())) # whole hours

# Per-node shapes and small-job walltime caps (h), from the NCI queue limits:
# https://opus.nci.org.au/spaces/Help/pages/236881198/Queue+Limits
# normalbw nodes are 128GB or 256GB; the conservative 128GB figure is used.
# minncpus marks queues whose smallest allowed request exceeds a quarter node.
const GADI_QUEUES = Dict(
    "normal" => (cores = 48, mem = 190, gpus = 0, walltime = 48),
    "express" => (cores = 48, mem = 190, gpus = 0, walltime = 24),
    "hugemem" => (cores = 48, mem = 1470, gpus = 0, walltime = 48),
    "megamem" => (cores = 48, mem = 2990, gpus = 0, walltime = 48),
    "gpuvolta" => (cores = 48, mem = 382, gpus = 4, walltime = 48),
    "dgxa100" => (cores = 128, mem = 2000, gpus = 8, walltime = 48),
    "gpuhopper" => (cores = 48, mem = 1024, gpus = 4, walltime = 48),
    "normalsr" => (cores = 104, mem = 500, gpus = 0, walltime = 48),
    "expresssr" => (cores = 104, mem = 500, gpus = 0, walltime = 24),
    "normalbw" => (cores = 28, mem = 128, gpus = 0, walltime = 48),
    "expressbw" => (cores = 28, mem = 128, gpus = 0, walltime = 24),
    "hugemembw" => (cores = 28, mem = 1020, gpus = 0, walltime = 48),
    "megamembw" => (cores = 64, mem = 3000, gpus = 0, walltime = 48, minncpus = 32),
    "normalsl" => (cores = 32, mem = 192, gpus = 0, walltime = 48),
)

"""
    gadi_defaults(queue; ncpus = nothing, mem = nothing, ngpus = nothing) -> NamedTuple

Resolve submission defaults `(; ncpus, mem, jobfs, ngpus, walltime)` for a Gadi
queue, completing any partial request SU-neutrally. Gadi charges
`walltime x rate x max(ncpus, mem / node_mem x node_cores)`, so:

- Nothing given: GPU queues get one GPU and its mandated core count (12 cpus
  per V100 on gpuvolta, 16 per A100 on dgxa100, 12 per H200 on gpuhopper); CPU
  queues a quarter node, raised to the queue's minimum request where larger
  (megamembw). Memory is the proportional node share rounded down, so the
  charge follows `ncpus`.
- `ncpus` (or `ngpus`) given: memory fills to the proportional share of those
  cores, keeping the charge at `ncpus`.
- `mem` given: cores fill to those the memory share already pays for, and a
  request beyond one node's memory rounds `ncpus` up to whole nodes. On GPU
  queues the GPU count fills the same way.
- Explicitly given fields are never altered.

Walltime defaults to the `gadi_maxwalltime_fraction` preference (env
`ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION`, accepting "0.25" or "1/4" forms;
default 1/4) of the queue's small-job cap, floored to whole hours: 12 of 48 on
`normal`, 6 of 24 on the express queues. Shorter requests backfill sooner and
SUs charge actual runtime, so the cap is rarely worth requesting up front.
`jobfs` is a flat 10GB on every queue: node-local disk does not affect the SU
charge but does constrain where a job can be placed, so proportional requests
would only make jobs harder to schedule; raise it for I/O-heavy work.

Unknown queues warn and fall back to the `normal` defaults.

# Examples
```julia
gadi_defaults("normal")             # (ncpus = 12, mem = 47, jobfs = 10, ngpus = 0, walltime = 12)
gadi_defaults("normal"; ncpus = 4)  # (ncpus = 4, mem = 15, ...): not the quarter-node 47GB
gadi_defaults("hugemem"; mem = 300) # (ncpus = 9, mem = 300, ...): the cores 300GB already pays for
gadi_defaults("gpuvolta"; ngpus = 2) # (ncpus = 24, mem = 191, ngpus = 2, ...)
```
"""
function gadi_defaults(
        queue::AbstractString;
        ncpus::Union{Integer, Nothing} = nothing,
        mem::Union{Real, AbstractString, Nothing} = nothing,
        ngpus::Union{Integer, Nothing} = nothing
    )
    queue = lowercase(queue)
    mem_gb = isnothing(mem) ? nothing : memory_string_to_gb(parse_memory(mem))
    # copyq is shaped unlike the compute queues (1 core, data-mover nodes)
    if queue == "copyq"
        return (;
            ncpus = something(ncpus, 1), mem = something(mem, 16),
            jobfs = 10, ngpus = 0, walltime = default_walltime(10),
        )
    end
    spec = get(GADI_QUEUES, queue, nothing)
    if isnothing(spec)
        @warn "Unknown Gadi queue '$queue'; using normal-queue defaults"
        spec = GADI_QUEUES["normal"]
    end
    if spec.gpus > 0
        ratio = spec.cores ÷ spec.gpus # mandated cpus per GPU
        if isnothing(ngpus)
            ngpus = if !isnothing(ncpus)
                max(1, ncpus ÷ ratio)
            elseif !isnothing(mem_gb)
                clamp(floor(Int, mem_gb / spec.mem * spec.gpus), 1, spec.gpus)
            else
                1
            end
        end
        ncpus = something(ncpus, ratio * max(ngpus, 1))
    else
        ngpus = something(ngpus, 0)
        if isnothing(ncpus)
            ncpus = if isnothing(mem_gb)
                spec.cores ÷ 4
            elseif mem_gb > spec.mem # beyond one node: whole nodes required
                ceil(Int, mem_gb / spec.mem) * spec.cores
            else
                clamp(floor(Int, mem_gb / spec.mem * spec.cores), 1, spec.cores)
            end
            ncpus = max(ncpus, get(spec, :minncpus, 1))
        end
    end
    mem = something(mem, max(1, floor(Int, spec.mem * ncpus / spec.cores)))
    return (; ncpus, mem, jobfs = 10, ngpus, walltime = default_walltime(spec.walltime))
end

"""
    pbs_script(; ID, julia_cmd, ncpus, mem, walltime, jobfs, ngpus, queue, project_code, storage, project) -> String

Render the `#PBS` submission script for one Gadi job. `mem`, `walltime`, and
`jobfs` must already be validated strings. `#PBS -V` is omitted (discouraged on
Gadi); the environment comes from sourcing `~/.bashrc` instead.
"""
function pbs_script(;
        ID, julia_cmd, ncpus, mem, walltime, jobfs, ngpus,
        queue, project_code, storage, project
    )
    storage_line = isempty(storage) ? "" : "\n#PBS -l storage=$(storage)"
    gpu_resource = ngpus > 0 ? ",ngpus=$(ngpus)" : ""
    return """
    #!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -P $(project_code)
    #PBS -q $(queue)
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$(ID).final.log
    #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem)),walltime=$(walltime),jobfs=$(lowercase(jobfs))$(gpu_resource)$(storage_line)
    source $(homedir())/.bashrc
    cd $(to_string(project))
    $(to_string(julia_cmd))
    """
end

"""
    runscript(script::String; kwargs...) -> (String, String)

Submit a Julia script as a PBS job on Gadi.

# Arguments
- `script::String`: Path to Julia script file
- `queue::String`: PBS queue; defaults to the `gadi_queue` preference, then "normal"
- `ncpus::Integer`: Number of CPUs
- `mem::Union{Real,String}`: Memory (number as GB or string with units)
- `ngpus::Integer`: GPUs; gpuvolta and gpuhopper require 12 cpus per GPU, dgxa100 16
- `defaults::NamedTuple=gadi_defaults(queue; ncpus, mem, ngpus)`: Resolved
  resources; unspecified members of `ncpus`/`mem`/`ngpus` are completed
  SU-neutrally from the queue shape (see `gadi_defaults`)
- `walltime::Union{Integer,String}`: Walltime (hours or "HH:MM:SS")
- `jobfs::Union{Real,String}`: Node-local scratch (the PBS default is 100MB)
- `project_code::String`: NCI project for `#PBS -P`; defaults to the `gadi_project` preference (required)
- `storage::String`: `#PBS -l storage=` declaration (e.g. "gdata/ab12+scratch/ab12"); defaults to the `gadi_storage` preference. Without it the job cannot see /g/data or /scratch.
- `qsubflags::Cmd=```: Additional qsub flags
- `project::Cmd`: Project directory; defaults to the active project's directory
- `exeflags::Cmd=```: Julia executable flags

# Returns
- Tuple of (job_id, logfile_path)

Resource defaults resolve per queue through `gadi_defaults`: one GPU on the GPU
queues, a quarter node otherwise, with memory just under the proportional share
so the SU charge follows `ncpus`, and walltime at the queue cap. Partial
requests complete SU-neutrally: given only `ncpus`, memory fills to that many
cores' share; given only `mem`, cores fill to those the memory already pays for.

# Examples
```julia
jobid, logfile = runscript("myscript.jl"; mem=64, walltime=12)
jobid, logfile = runscript("myscript.jl"; queue="express", storage="gdata/ab12")
jobid, logfile = runscript("train.jl"; queue="gpuvolta")  # 1 GPU, 12 cpus, 95GB
jobid, logfile = runscript("myscript.jl"; ncpus=4)        # 4 cpus, 15GB, not 47GB
```
"""
function runscript(
        script::String;
        queue::AbstractString = pref_or_env("gadi_queue", "normal"),
        ncpus::Union{Integer, Nothing} = nothing,
        mem::Union{Real, AbstractString, Nothing} = nothing,
        ngpus::Union{Integer, Nothing} = nothing,
        defaults::NamedTuple = gadi_defaults(queue; ncpus, mem, ngpus),
        walltime::Union{Integer, AbstractString} = defaults.walltime,
        jobfs::Union{Real, AbstractString} = defaults.jobfs,
        project_code::AbstractString = gadi_project(),
        storage::AbstractString = something(pref_or_env("gadi_storage"), ""),
        qsubflags::Cmd = ``,
        project::Cmd = default_project(),
        exeflags::Cmd = ``,
        kwargs...
    )
    mem_str = parse_memory(defaults.mem)
    walltime_str = parse_walltime(walltime)
    jobfs_str = parse_memory(jobfs)

    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}.$(ID).log`
    exeflags = with_heap_hint(exeflags, mem_str)

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)
    cmd = pbs_script(;
        ID, julia_cmd, ncpus = defaults.ncpus, mem = mem_str,
        walltime = walltime_str, jobfs = jobfs_str, ngpus = defaults.ngpus,
        queue, project_code, storage, project
    )

    qsub_file = first(mktemp(LOGDIR; cleanup = false))
    write(qsub_file, cmd)

    jobid, server = capture_jobid(`qsub $(qsubflags) $(qsub_file)`) # local: submission happens on Gadi
    return jobid, replace(to_string(logfile), r"\$\{PBS_JOBID\}" => "$jobid.$server")
end

"""
    runscript(expr::Expr; kwargs...) -> (String, String)

Submit a Julia expression as a PBS job on Gadi.

# Arguments
- `expr::Expr`: Julia expression to execute
- `setup::Expr`: Optional block prepended to `expr` (activation/`using` boilerplate)
- kwargs: Same as `runscript(::String)`

# Returns
- Tuple of (job_id, logfile_path)
"""
function runscript(expr::Expr; setup::Union{Expr, Nothing} = nothing, kwargs...)
    file = first(mktemp(LOGDIR; cleanup = false))
    write_exprs(file, combine_exprs(setup, expr))
    return runscript(file; kwargs...)
end

"""
    runscripts(exprs::Vector; kwargs...) -> Vector{Tuple{String, String}}

Submit multiple Julia expressions as independent PBS jobs on Gadi. Gadi rejects
PBS job arrays (`#PBS -J`), so each expression is written to a numbered script
and submitted separately; each job is independently scheduled and qdel-able.

# Arguments
- `exprs::Vector`: Vector of Julia expressions
- `setup::Expr`: Optional block prepended to every expression, for the
  activation/`using` boilerplate each job repeats
- kwargs: Same as `runscript(::String)`

# Returns
- Vector of (job_id, logfile_path) tuples, in input order

# Examples
```julia
exprs = [:(main(\$a, \$b)) for (a, b) in Iterators.product(0.1:0.1:1, 1:20)] |> vec
jobs = runscripts(exprs; setup = quote
    using DrWatson
    @quickactivate :MyPackage
end)
```
"""
function runscripts(exprs::Vector; setup::Union{Expr, Nothing} = nothing, kwargs...)
    ID = "runscripts_$(next_runscripts_id())"
    scriptdir = "$(LOGDIR)/$(ID).script"
    mkpath(expanduser(scriptdir))

    foreach(enumerate(exprs)) do (i, ex)
        write_exprs(expanduser("$(scriptdir)/$i.jl"), combine_exprs(setup, ex))
    end
    return runscripts(scriptdir; kwargs...)
end

"""
    runscripts(scriptdir::String; kwargs...) -> Vector{Tuple{String, String}}

Submit a directory of numbered Julia scripts (1.jl, 2.jl, ...) as independent
PBS jobs on Gadi; only files named `<integer>.jl` are submitted.

# Arguments
- `scriptdir::String`: Directory containing numbered Julia scripts
- kwargs: Same as `runscript(::String)`

# Returns
- Vector of (job_id, logfile_path) tuples, in numerical order
"""
function runscripts(scriptdir::String; kwargs...)
    files = filter(f -> occursin(r"^\d+\.jl$", f), readdir(scriptdir)) # ignores stray files
    isempty(files) && throw(ArgumentError("No scripts found in directory: $scriptdir"))
    sort!(files; by = f -> parse(Int, first(splitext(f))))
    return map(f -> runscript(joinpath(scriptdir, f); kwargs...), files)
end

# ===========================
# Distributed within a job
# ===========================

# Unique hosts of a PBS nodefile in order (launch node first); hosts repeat
# per mpiprocs rank, so dedup
function nodefile_hosts(lines)
    hosts = String[]
    for l in lines
        h = strip(l)
        (isempty(h) || h in hosts) || push!(hosts, String(h))
    end
    return hosts
end

# Allocated cpus: PBS_NCPUS on Gadi; nodefile lines otherwise (per-rank listing)
function job_slots(lines)
    return something(
        tryparse(Int, get(ENV, "PBS_NCPUS", "")),
        count(!isempty ∘ strip, lines)
    )
end

# np workers over hosts, differing by at most one; earlier hosts take the remainder
function even_split(np, hosts)
    n = length(hosts)
    return [h => np ÷ n + (i <= np % n) for (i, h) in enumerate(hosts)]
end

"""
    distributeprocs(np = ncpus ÷ threads; threads = 1, kwargs...) -> Vector{Int}

Launch `np` workers spread evenly across the nodes of the current PBS job, read
from `\$PBS_NODEFILE`. Bare `Distributed.addprocs(n)` puts every worker on the
launch node, oversubscribing it while the job's other nodes sit idle; here
workers on the launch node start locally and remote nodes are reached over the
job's intra-job ssh access. Each worker runs `threads` Julia threads with
`OPENBLAS_NUM_THREADS` to match (OpenBLAS otherwise starts a thread per node
core in every worker), so the default fills the allocation with one
single-threaded worker per allocated cpu.

# Arguments
- `np`: Total workers; defaults to allocated cpus ÷ `threads`
- `threads::Integer=1`: Julia (and OpenBLAS) threads per worker
- `project=dirname(Base.active_project())`: Project directory for workers
- `sshflags::Cmd`: Flags for the ssh launch onto remote nodes
- `kwargs...`: Forwarded to `Distributed.addprocs`

# Examples
```julia
procs = distributeprocs()              # one single-threaded worker per cpu
procs = distributeprocs(24)            # 24 workers, spread evenly
procs = distributeprocs(; threads = 4) # 4-threaded workers, cpus ÷ 4 of them
```
"""
function distributeprocs(
        np::Union{Integer, Nothing} = nothing;
        threads::Integer = 1,
        project = dirname(Base.active_project()),
        sshflags::Cmd = `-o StrictHostKeyChecking=accept-new`,
        kwargs...
    )
    haskey(ENV, "PBS_NODEFILE") ||
        error("PBS_NODEFILE is not set; distributeprocs must run inside a PBS job")
    lines = readlines(ENV["PBS_NODEFILE"])
    hosts = nodefile_hosts(lines)
    isempty(hosts) && error("No hosts found in $(ENV["PBS_NODEFILE"])")
    slots = job_slots(lines)
    np = something(np, max(1, slots ÷ threads))
    np > 0 || throw(ArgumentError("np must be positive, got $np"))
    np * threads > slots &&
        @warn "$np workers x $threads threads exceeds the $slots allocated cpus"

    alloc = even_split(np, hosts)
    @info "distributeprocs allocation" alloc

    exename = joinpath(Sys.BINDIR, "julia") # shared filesystem: same binary everywhere
    exeflags = `--project=$(project) -t $(threads)`
    env = ["OPENBLAS_NUM_THREADS" => string(threads)]
    me = first(split(gethostname(), '.')) # nodefile uses short hostnames
    procs = Int[]
    for (h, n) in alloc
        n > 0 || continue
        new = if h == me
            # restrict=false: workers must be reachable from the other nodes
            Distributed.addprocs(n; restrict = false, exeflags, env, kwargs...)
        else
            Distributed.addprocs(
                [(h, n)]; exename, exeflags, env, sshflags, dir = pwd(),
                kwargs...
            )
        end
        append!(procs, new)
    end
    return procs
end

"""
    selfdestruct()

Terminate the current PBS job from within the job itself.

This function will send a qdel command to remove the current job.
Use with caution as it immediately terminates the job.
"""
function selfdestruct()
    if !haskey(ENV, "PBS_JOBID")
        @warn "Not running in a PBS job environment"
        return
    end
    jobid = ENV["PBS_JOBID"]
    @info "Nuking job $jobid"
    run(`qdel $jobid`) # PBS client tools are available on Gadi compute nodes
    return @info "Nuked job $jobid."  # This likely won't run if successful
end

end # module NCIGadi
