module USydClusters
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

function build_julia_command(; exename = `julia`, exeflags = ``,
                             project = ``, args = ``,
                             script, logfile)
    mkpath(LOGDIR)
    if !(args isa String)
        args = join(args, " ")
    end
    `$exename $exeflags -t auto --project=$project $script $args 2\>\&1 \| tee "$logfile"`
end
to_string(cmd::Cmd) = join(cmd.exec, " ")

include("USydPhysics.jl")
end
