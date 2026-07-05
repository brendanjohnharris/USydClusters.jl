module AcademicClusters
using Preferences

"""
Load a preference or environment variable with priority preference -> env -> default
"""
function pref_or_env(key, default = nothing)
    env = "AcademicClusters_$(uppercase(key))"
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

include("USydPhysics.jl")
end
