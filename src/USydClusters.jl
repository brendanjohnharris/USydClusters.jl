module USydClusters
using Dates
using Preferences

"""
Load a preference or environment variable with priority preference -> env -> default
"""
function pref_or_env(key, default = nothing)
    env = "USYDCLUSTERS_$(uppercase(key))"
    pref = lowercase(key)

    if @has_preference(pref)
        @load_preference(pref)
    elseif haskey(ENV, env)
        ENV[env]
    else
        default
    end
end

begin # * Preferences
    const LOGDIR = rstrip(pref_or_env("logdir", "/tmp/jobs/"), '/')
end

parse_gb(mem::Integer) = mem
function parse_gb(mem)
    throw(ArgumentError("Memory must be an Integer. Strings and decimals are not implemented yet"))
end
# * May, at some stage, want to add a Unitful extension that parses strings or Units and
#   converts to GB

parse_walltime(walltime::String) = walltime
parse_walltime(walltime::Integer) = "$(walltime):00:00"
parse_walltime(walltime::TimePeriod) = parse_walltime(walltime + Time(0))
parse_walltime(walltime::Time) = Dates.format(walltime, "HH:MM:SS")

function format_pbs_resources(ncpus, mem, walltime)
    mem = parse_gb(mem)
    walltime = parse_walltime(walltime)
    """#PBS -l select=1:ncpus=$(ncpus):mem=$(mem)GB:vmem=$(mem)GB
    #PBS -l walltime=$(walltime)"""
end

function build_julia_command(; exename = `julia`, exeflags = ``,
                             project = ``, mem_sandbox = false, args = ``,
                             script, logfile)
    mkpath(LOGDIR)
    if isnothing(mem_sandbox) || mem_sandbox === false
        sandbox = ``
    else
        KB = mem_sandbox * 1024 * 1024
        sandbox = `ulimit -m $(KB) -v $(KB)\;`
    end
    if !(args isa String)
        args = join(args, " ")
    end
    `$(sandbox) $exename $exeflags -t auto --project=$project $script $args 2\>\&1 \| tee $logfile`
end

to_string(cmd::Cmd) = join(cmd.exec, " ")

include("Physics.jl")
include("RemotePhysics.jl")
end
