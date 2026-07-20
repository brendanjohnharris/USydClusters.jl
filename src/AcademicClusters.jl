module AcademicClusters
using Preferences

"""
Load a preference or environment variable with priority preference -> env -> default
"""
function pref_or_env(key, default = nothing)
    env = "ACADEMICCLUSTERS_$(uppercase(key))"
    pref = lowercase(key)

    return if @has_preference(pref)
        @load_preference(pref)
    elseif haskey(ENV, env)
        ENV[env]
    else
        default
    end
end

begin # * Preferences
    # Default must be shared storage: qsub runs on the headnode and array tasks on
    # compute nodes, none of which see the submit host's node-local /tmp.
    const LOGDIR = rstrip(pref_or_env("logdir", joinpath(homedir(), ".jobs")), '/')
end

function build_julia_command(;
        exename = `julia`, exeflags = ``,
        project = ``, args = ``,
        script, logfile
    )
    mkpath(LOGDIR)
    if !(args isa String)
        args = join(args, " ")
    end
    return `$exename $exeflags -t auto --project=$project $script $args 2\>\&1 \| tee "$logfile"`
end
to_string(cmd::Cmd) = join(cmd.exec, " ")

# Directory of the active project, as a Cmd for interpolation into job scripts;
# falls back to the working directory when no project is active
function default_project()
    project = Base.active_project()
    return `$(isnothing(project) ? pwd() : dirname(project))`
end

# ===========================
# PBS-generic helpers, shared by the site modules
# ===========================

"""
    parse_memory(mem::Union{Real, AbstractString}) -> String

Parse memory specification and return optimal unit representation.

# Arguments
- `mem`: Memory specification as number (GB) or string with units

# Returns
- Memory string with optimal units (e.g., "16GB", "500MB", "123KB")

# Examples
```julia
parse_memory(1)         # "1GB"
parse_memory(0.5)       # "512MB"
parse_memory(16)        # "16GB"
parse_memory(1.5)       # "1536MB"
parse_memory("123KB")   # "123KB"
parse_memory("16GB")    # "16GB"
```
"""
function parse_memory(mem::Real)
    mem > 0 || throw(ArgumentError("Memory must be positive, got $mem GB"))
    if mem >= 1 && isinteger(mem)
        return "$(Int(mem))GB"
    else
        return "$(Int(round(mem * 1024)))MB" # fractional GB expressed in MB
    end
end

function parse_memory(mem::AbstractString)
    mem = strip(mem)
    if !occursin(r"^\d+(?:\.\d+)?[KMGT]B$"i, mem) # no internal whitespace: string is embedded raw in `#PBS -l`
        throw(ArgumentError("Invalid memory format: '$mem'. Use format like '16GB', '2048MB', '512KB', or '2TB'"))
    end
    return mem # preserve the user's choice of units
end

"""
    parse_walltime(walltime::Union{Integer, AbstractString}) -> String

Parse walltime specification into HH:MM:SS format string.

# Arguments
- `walltime`: Time specification as integer (hours) or string in HH:MM:SS format

# Returns
- Walltime in "HH:MM:SS" format

# Examples
```julia
parse_walltime(24)          # "24:00:00"
parse_walltime(1)           # "01:00:00"
parse_walltime(168)         # "168:00:00" (1 week)
parse_walltime("24:00:00")  # "24:00:00"
parse_walltime("01:30:00")  # "01:30:00"
parse_walltime("00:45:30")  # "00:45:30"
```
"""
function parse_walltime(walltime::Integer)
    walltime > 0 || throw(ArgumentError("Walltime must be positive, got $walltime hours"))
    return "$(lpad(walltime, 2, '0')):00:00" # PBS allows hours > 99, so no 2-digit cap
end

function parse_walltime(walltime::AbstractString)
    walltime = strip(walltime)
    if !occursin(r"^\d+:\d{2}:\d{2}$", walltime)
        throw(ArgumentError("Invalid walltime format: '$walltime'. Expected format: HH:MM:SS (e.g., '24:00:00', '168:00:00')"))
    end

    hours, minutes, seconds = parse.(Int, split(walltime, ':'))
    0 <= minutes <= 59 || throw(ArgumentError("Minutes must be between 0 and 59"))
    0 <= seconds <= 59 || throw(ArgumentError("Seconds must be between 0 and 59"))
    hours * 3600 + minutes * 60 + seconds > 0 ||
        throw(ArgumentError("Total walltime must be greater than 0"))

    return walltime
end

"""
    memory_string_to_gb(mem_str::AbstractString) -> Float64

Extract numeric GB value from memory string for calculations.

# Arguments
- `mem_str`: Memory string with units (e.g., "16GB", "2048MB")

# Returns
- Memory value in GB as Float64

# Internal use only - for heap size calculations
"""
function memory_string_to_gb(mem_str::AbstractString) # parse_memory returns SubString
    m = match(r"^(\d+(?:\.\d+)?)\s*([KMGT]B)$"i, mem_str)
    isnothing(m) && return 16.0  # default fallback
    factor = Dict("KB" => 1 / 1024^2, "MB" => 1 / 1024, "GB" => 1.0, "TB" => 1024.0)
    return parse(Float64, m[1]) * factor[uppercase(m[2])]
end

# Workers get a heap-size hint of half their requested memory
with_heap_hint(exeflags::Cmd, mem_str) = `$exeflags --heap-size-hint=$(ceil(Int, memory_string_to_gb(mem_str) / 2))G`

"""
    capture_jobid(cmd::Cmd) -> (String, String)

Run a qsub command and parse (jobid, server) from its 'JOBID.server' output.
Array-job brackets are stripped from the ID.
"""
function capture_jobid(cmd::Cmd)
    output = strip(read(cmd, String))
    parts = split(output, '.')
    length(parts) >= 2 ||
        error("Unexpected qsub output: '$output'. Expected 'JOBID.server'.")
    jobid = replace(first(parts), "[]" => "")
    isnothing(tryparse(Int, jobid)) &&
        error("Job id could not be parsed from qsub output '$output'. Please check your `.bashrc` file doesn't print to stdout.")
    return jobid, join(parts[2:end], '.')
end

# Write an expression to a script file, one top-level statement per line
function write_exprs(file, ex::Expr)
    stmts = ex.head === :block ? Base.remove_linenums!(ex).args : [ex]
    return open(file, "w") do f
        foreach(stmt -> println(f, stmt), stmts)
    end
end

blockargs(ex::Expr) = ex.head === :block ? ex.args : Any[ex]

"""
    combine_exprs(setup, ex)

Prepend a shared `setup` block to a job expression; `nothing` passes `ex`
through unchanged.
"""
combine_exprs(setup::Expr, ex::Expr) = Expr(:block, blockargs(setup)..., blockargs(ex)...)
combine_exprs(::Nothing, ex::Expr) = ex

function next_runscripts_id(dir = LOGDIR)
    dir = expanduser(dir)
    isdir(dir) || return 1
    re = r"^runscripts_(\d+)\.script$"
    ids = Int[]
    for entry in readdir(dir)
        m = match(re, entry)
        m === nothing || push!(ids, parse(Int, m.captures[1]))
    end
    return isempty(ids) ? 1 : maximum(ids) + 1
end

include("USydPhysics.jl")
include("NCIGadi.jl")
end
