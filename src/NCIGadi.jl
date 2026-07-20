module NCIGadi
import AcademicClusters: pref_or_env, build_julia_command, LOGDIR, to_string,
    parse_memory, parse_walltime, with_heap_hint, capture_jobid, write_exprs,
    combine_exprs, next_runscripts_id, default_project

export runscript, runscripts, selfdestruct, gadi_defaults

# No addprocs/ClusterManager here: Gadi compute nodes cannot be reached for the
# Distributed handshake, so only batch submission is supported.

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
    gadi_defaults(queue) -> NamedTuple

Resolve per-queue submission defaults `(; ncpus, mem, jobfs, ngpus, walltime)`
from the Gadi queue shapes in `GADI_QUEUES`. GPU queues default to one GPU and
its mandated core count (12 cpus per V100 on gpuvolta, 16 per A100 on dgxa100,
12 per H200 on gpuhopper); CPU queues default to a quarter node, raised to the
queue's minimum request where larger (megamembw), with memory the proportional
node share rounded down so the SU charge follows `ncpus`. Walltime is the queue's
small-job cap (48 hours; 24 on the express queues). `jobfs` is a flat 10GB on
every queue: node-local disk does not affect the SU charge but does constrain
where a job can be placed, so proportional requests would only make jobs harder
to schedule; raise it for I/O-heavy work.

Unknown queues warn and fall back to the `normal` defaults.

# Examples
```julia
gadi_defaults("normal")    # (ncpus = 12, mem = 47, jobfs = 10, ngpus = 0, walltime = 48)
gadi_defaults("gpuvolta")  # (ncpus = 12, mem = 95, jobfs = 10, ngpus = 1, walltime = 48)
gadi_defaults("dgxa100")   # (ncpus = 16, mem = 250, jobfs = 10, ngpus = 1, walltime = 48)
```
"""
function gadi_defaults(queue::AbstractString)
    queue = lowercase(queue)
    # copyq is shaped unlike the compute queues (1 core, data-mover nodes)
    queue == "copyq" && return (; ncpus = 1, mem = 16, jobfs = 10, ngpus = 0, walltime = 10)
    spec = get(GADI_QUEUES, queue, nothing)
    if isnothing(spec)
        @warn "Unknown Gadi queue '$queue'; using normal-queue defaults"
        spec = GADI_QUEUES["normal"]
    end
    ngpus = spec.gpus > 0 ? 1 : 0
    ncpus = ngpus > 0 ? spec.cores ÷ spec.gpus :
        max(1, spec.cores ÷ 4, get(spec, :minncpus, 1))
    return (;
        ncpus,
        mem = max(1, floor(Int, spec.mem * ncpus / spec.cores)),
        jobfs = 10,
        ngpus,
        walltime = spec.walltime,
    )
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
- `defaults::NamedTuple=gadi_defaults(queue)`: Per-queue resource defaults
- `ncpus::Integer`: Number of CPUs
- `mem::Union{Real,String}`: Memory (number as GB or string with units)
- `walltime::Union{Integer,String}`: Walltime (hours or "HH:MM:SS")
- `jobfs::Union{Real,String}`: Node-local scratch (the PBS default is 100MB)
- `ngpus::Integer`: GPUs; gpuvolta and gpuhopper require 12 cpus per GPU, dgxa100 16
- `project_code::String`: NCI project for `#PBS -P`; defaults to the `gadi_project` preference (required)
- `storage::String`: `#PBS -l storage=` declaration (e.g. "gdata/ab12+scratch/ab12"); defaults to the `gadi_storage` preference. Without it the job cannot see /g/data or /scratch.
- `qsubflags::Cmd=```: Additional qsub flags
- `project::Cmd`: Project directory; defaults to the active project's directory
- `exeflags::Cmd=```: Julia executable flags

# Returns
- Tuple of (job_id, logfile_path)

Resource defaults resolve per queue through `gadi_defaults`: one GPU on the GPU
queues, a quarter node otherwise, with memory just under the proportional share
so the SU charge follows `ncpus`, and walltime at the queue cap.

# Examples
```julia
jobid, logfile = runscript("myscript.jl"; mem=64, walltime=12)
jobid, logfile = runscript("myscript.jl"; queue="express", storage="gdata/ab12")
jobid, logfile = runscript("train.jl"; queue="gpuvolta")  # 1 GPU, 12 cpus, 95GB
```
"""
function runscript(
        script::String;
        queue::AbstractString = pref_or_env("gadi_queue", "normal"),
        defaults::NamedTuple = gadi_defaults(queue),
        ncpus::Integer = defaults.ncpus,
        mem::Union{Real, AbstractString} = defaults.mem,
        walltime::Union{Integer, AbstractString} = defaults.walltime,
        jobfs::Union{Real, AbstractString} = defaults.jobfs,
        ngpus::Integer = defaults.ngpus,
        project_code::AbstractString = gadi_project(),
        storage::AbstractString = something(pref_or_env("gadi_storage"), ""),
        qsubflags::Cmd = ``,
        project::Cmd = default_project(),
        exeflags::Cmd = ``,
        kwargs...
    )
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)
    jobfs_str = parse_memory(jobfs)

    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}.$(ID).log`
    exeflags = with_heap_hint(exeflags, mem_str)

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)
    cmd = pbs_script(;
        ID, julia_cmd, ncpus, mem = mem_str, walltime = walltime_str,
        jobfs = jobfs_str, ngpus, queue, project_code, storage, project
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
