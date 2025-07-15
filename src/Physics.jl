module Physics
using Distributed
using ClusterManagers
import USydClusters: build_julia_command, format_pbs_resources, LOGDIR, to_string
import ClusterManagers.worker_arg
import ClusterManagers.ClusterManager
import ClusterManagers.WorkerConfig

export PBSProManager, addprocs
struct PBSProManager <: ClusterManager
    np::Integer
    ncpus::Integer
    mem::Integer # GB
    walltime::Integer # Hours
    queue::Any
    project::Any
    qsubflags::Any
end
function PBSProManager(np, ncpus, mem, walltime, queue; kwargs...)
    PBSProManager(np, ncpus, mem, walltime, queue, ``, ``, ``; kwargs...)
end
function PBSProManager(np = 1; ncpus = 8, mem = 16, walltime = 24, queue = ``,
                       project = ``, qsubflags = ``, kwargs...)
    return PBSProManager(np, ncpus, mem, walltime, queue, project, qsubflags;
                         kwargs...)
end

function ClusterManagers.manage(manager::PBSProManager,
                                id::Int64, config::WorkerConfig, op::Symbol)
end

function ClusterManagers.launch(manager::PBSProManager,
                                params::Dict, instances_arr::Array, c::Condition)
    try
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]

        np = manager.np
        ncpus = manager.ncpus
        mem = manager.mem
        walltime = manager.walltime
        queue = manager.queue
        if isnothing(queue)
            queue = ``
        end
        if !isempty(queue)
            queue = `-q $(queue)`
        end

        project = manager.project
        qsubflags = manager.qsubflags
        @debug "Activating worker project $project"

        ID = getpid()
        if isempty(project) || isnothing(project)
            project = dirname(Base.active_project())
        end

        if np == 1
            Jcmd = ""
            logdir = `$(LOGDIR)`
            logfile = `$(to_string(logdir))/\$\{MAIN_JOBID\}.$(ID).log`
        elseif np > 1
            Jcmd = "#PBS -J 1-$np"
            logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
            logfile = `$(to_string(logdir)).\$\{PBS_ARRAY_INDEX\}.log`
        else
            throw(ArgumentError("np must be a positive integer, got $np"))
        end

        script = ClusterManagers.worker_arg()
        exeflags = `$exeflags --heap-size-hint=$(ceil(Int, mem/2))G`
        julia_cmd = build_julia_command(; exename, exeflags, project, script, logfile)

        ID = Base.shell_escape("$(ID)")
        cmd = """#!/bin/bash
        #PBS -N julia-$ID
        #PBS -V
        #PBS -j oe
        #PBS -m n
        #PBS -o $(LOGDIR)/$ID.final.log
        $(Jcmd)
        $(format_pbs_resources(ncpus, mem, walltime))
        cd $dir
        source $(ENV["HOME"])/.bashrc
        MAIN_JOBID=\${PBS_JOBID%\\[*}
        MAIN_JOBID=\${MAIN_JOBID%.*}
        mkdir -p "$(to_string(logdir))"
        $(to_string(julia_cmd))
        """
        @debug cmd

        f = tempname(LOGDIR)
        write(f, cmd)

        @debug(cmd)
        _qsub = `/usr/physics/pbspro/bin/qsub $(queue) $(qsubflags)`
        qsub = "source $(ENV["HOME"])/.bashrc > /dev/null && $(Base.shell_escape(_qsub)) $(Base.shell_escape(f))"

        qsub_cmd = pipeline(`ssh headnode "$qsub"`)
        @debug qsub_cmd
        out = open(qsub_cmd)
        @debug out
        if !success(out)
            throw(error()) # qsub already gives a message
        end
        line = readline(out)
        id = split(line, '.') |> first |> chomp
        id = replace(id, r"\[\]" => "")
        @debug id

        # * Reconstruct actual log file names from jobid
        if np == 1
            logfile = `$(to_string(logdir))/$id.$(ID).log`
            fnames = ["$(to_string(logfile))"]
        else
            logdir = `$(LOGDIR)/$id\[\].$(ID).log`
            fnames = ["$(to_string(logdir)).$i.log" for i in 1:np]
        end

        if endswith(id, "[]")
            id = id[1:(end - 2)]
        end
        if isnothing(tryparse(Int, id))
            error("Job id could not be parse from worker output '$line'. Please make sure tour `.bashrc` file does not print anything to stdout.")
        end

        println("Job $id in queue.")
        for i in 1:np
            # wait for each output stream file to get created
            fname = fnames[i]
            if haskey(ENV, "JULIA_WORKER_TIMEOUT")
                hosttimeout = tryparse(Int, ENV["JULIA_WORKER_TIMEOUT"])
            else
                hosttimeout = 480
            end
            start_time = time()
            while !isfile(fname) && (time() - start_time) < hosttimeout
                # @debug "Waiting for worker $i to connect at $fnames"
                sleep(1)
            end
            !isfile(fname) &&
                error("Worker $i did not connect at $fname after $hosttimeout seconds.")

            # Hack to get Base to get the host:port, the Julia process has already started.
            # cmd = `tail -f $fname`
            host = readline(fname)
            start_time = time()
            while isempty(host) && (time() - start_time) < hosttimeout
                sleep(1)
                # @debug "Waiting for worker $i to write hostname to $fname"
                host = readline(fname)
            end
            isempty(host) &&
                error("Hostname not written to file after $hosttimeout seconds.")
            host = split(host, ['#', ':'])
            port = Meta.parse(host[2])
            host = host[3]

            config = WorkerConfig()

            # config.io = open(detach(cmd))
            config.host = host
            config.port = port

            config.userdata = Dict{Symbol, Any}(:job => id, :task => i, :iofile => fname)
            push!(instances_arr, config)
            notify(c)
        end
        rm(f, force = true)
        logloc = np == 1 ? logfile : logdir
        println("Running. See stdout of children at $logloc")

    catch e
        println("Error launching workers")
        println(e)
        rm(f, force = true)
    end
end

function ClusterManagers.kill(manager::PBSProManager, id::Int64, config::WorkerConfig)
    @debug "Killing process $id"
    remotecall(exit, id)
end

function addprocs(np::Integer; ncpus = 8, mem = 16, walltime = 24, queue = ``, project = ``,
                  qsubflags = ``, kwargs...)
    ClusterManagers.addprocs(PBSProManager(np; ncpus, mem, walltime, queue, project,
                                           qsubflags);
                             enable_threaded_blas = true,
                             kwargs...)
end

# function addprocs(f::Function; preamble = nothing, args = (), kwargs = (;), _kwargs...)
#     p = addprocs(1; _kwargs...) |> only
#     try
#         if !isnothing(preamble)
#             if !(preamble isa Expr)
#                 preamble = Meta.parse(preamble)
#             end
#             o = @spawnat p eval(preamble)
#             wait(o)
#         end
#         o = remotecall_fetch(f, p, args...; kwargs...)
#         @debug "Worker $p completed successfully, removing."
#         # pbsid = Distributed.map_pid_wrkr[p].config.userdata[:job]
#         # run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
#     catch e
#         @error "Error in worker $p: $e"
#         o = nothing
#     finally
#         rmprocs(p)
#         @debug "Worker $p removed successfully."
#     end
#     return o
# end
# function addprocs(f::Function, itr; preamble = nothing, args = (), kwargs = (;), _kwargs...)
#     O = Vector{Any}(undef, length(itr))
#     procs = addprocs(length(itr); _kwargs...)
#     if !isnothing(preamble)
#         if !(preamble isa Expr)
#             preamble = Meta.parse(preamble)
#         end
#         @everywhere procs $preamble
#     end
#     @sync for i in eachindex(itr)
#         p = procs[i]
#         # function func(f, args...; kwargs...)
#         #     try
#         #         f(args...; kwargs...)
#         #     catch e
#         #         e
#         #     end
#         # end
#         o = @async remotecall_fetch(f, p, (itr[i], args...); kwargs...)
#         O[i] = o
#     end
#     @info "Workers completed successfully, removing."
#     for p in procs
#         pbsid = Distributed.map_pid_wrkr[p].config.userdata[:job]
#         run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
#         @info "Job $pbsid removed successfully."
#     end
#     return O
# end
# function addprocs(f::Function, itr, batchsize::Integer; args = (), kwargs = (;),
#                   _kwargs...)
#     if batchsize == 1
#         O = Vector{Any}(undef, length(itr))
#         for i in eachindex(itr)
#             o = @async addprocs(f; args = (itr[i], args...), kwargs = kwargs, _kwargs...)
#             O[i] = o
#         end
#     else # This helps because precompilation always takes place on the calling process, so want to limit the number of times it happens, but still asynchronously start jobs
#         batches = collect(Iterators.partition(eachindex(itr), batchsize))
#         O = Vector{Any}(undef, length(batches))
#         for bi in eachindex(batches)
#             o = @async addprocs(f, itr[batches[bi]]; args, kwargs = kwargs, _kwargs...)
#             O[bi] = o
#         end
#     end
#     return O
# end

function capture_jobid(cmd)
    output = read(cmd, String)
    jobid, hostname = split(output, '.')
    return jobid
end

function runscript(script::String;
                   ncpus = 10,
                   mem = 31,
                   walltime = 48,
                   qsubflags = "",
                   project = ``,
                   exeflags = ``,
                   queue = ``,
                   kwargs...)
    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}.$(ID).log`
    exeflags = `$exeflags --heap-size-hint=$(ceil(Int, mem/2))G`

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)

    cmd = """#!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$ID.final.log
    $(format_pbs_resources(ncpus, mem, walltime))
    source $(ENV["HOME"])/.bashrc
    cd $project
    $(to_string(julia_cmd))
    """
    qsub_file = first(mktemp(LOGDIR; cleanup = false))
    open(qsub_file, "w") do f
        write(f, cmd)
    end
    queue = isempty(queue) ? queue : "-q $(Base.shell_escape(queue))"
    qsub = "source $(ENV["HOME"])/.bashrc && /usr/physics/pbspro/bin/qsub $(string(qsubflags)) $queue $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    jobid = capture_jobid(qsub_cmd)
    return jobid, replace(to_string(logfile), r"\$\{PBS_JOBID\}" => "$jobid.headnode")
end

function runscript(expr::Expr; kwargs...)
    file = first(mktemp(LOGDIR; cleanup = false))
    open(file, "w") do f
        write(f, string(expr))
    end
    runscript(file; kwargs...)
end

function runscripts(exprs; kwargs...)
    ID = rand(UInt16) |> Int
    ID = "runscripts_$(ID)"

    scriptdir = "$(LOGDIR)/$(ID).script"

    mkpath(expanduser(scriptdir))
    scriptfiles = map(enumerate(exprs)) do (i, ex)
        file = expanduser("$(scriptdir)/$i.jl")
        open(file, "w") do f
            write(f, string(ex))
        end
        return file
    end
    runscripts(scriptdir; ID, kwargs...)
end

function runscripts(scriptdir::String; # The files should be named 1.jl, 2.jl, etc.
                    ncpus = 10,
                    mem = 31,
                    walltime = 48,
                    qsubflags = "",
                    project = ``,
                    exeflags = ``,
                    queue = ``,
                    ID = rand(UInt16) |> Int,
                    kwargs...)
    script = `$(scriptdir)/\$\{PBS_ARRAY_INDEX\}.jl`
    logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
    logfile = `$(to_string(logdir))/\$\{PBS_ARRAY_INDEX\}.log`
    exeflags = `$exeflags --heap-size-hint=$(ceil(Int, mem/2))G`
    N = length(readdir(scriptdir))

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)
    cmd = """#!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$ID.final.log
    $(format_pbs_resources(ncpus, mem, walltime))
    #PBS -J 1-$N
    source $(ENV["HOME"])/.bashrc
    cd $project
    MAIN_JOBID=\${PBS_JOBID%\\[*}
    MAIN_JOBID=\${MAIN_JOBID%.*}
    mkdir -p "$(to_string(logdir))"
    $(to_string(julia_cmd))
    """

    qsub_file = first(mktemp(scriptdir; cleanup = false))
    open(qsub_file, "w") do f
        write(f, cmd)
    end
    queue = isempty(queue) ? queue : "-q $(Base.shell_escape(queue))"
    qsub = "source $(ENV["HOME"])/.bashrc && /usr/physics/pbspro/bin/qsub $(string(qsubflags)) $queue $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    @info "Submitting array job with name julia-$ID (logdir: $LOGDIR)"
    jobid = capture_jobid(qsub_cmd)
    return jobid
end

function selfdestruct()
    pbsid = split(ENV["PBS_JOBID"], ".") |> first
    @info "Nuking job $pbsid"
    run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
    @info "Nuked job $pbsid." # All going well, this won't run
end
end # module
