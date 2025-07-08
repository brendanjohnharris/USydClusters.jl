module Physics
using Distributed
using ClusterManagers
import .USydClusters: build_julia_command, format_pbs_resources, LOGDIR, to_string
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
end
function PBSProManager(np, ncpus, mem, walltime, queue; kwargs...)
    PBSProManager(np, ncpus, mem, walltime, queue, ``; kwargs...)
end

function ClusterManagers.launch(manager::PBSProManager,
                                params::Dict, instances_arr::Array, c::Condition)
    try
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]
        mem_sandbox = params[:mem_sandbox]

        np = manager.np
        ncpus = manager.ncpus
        mem = manager.mem
        walltime = manager.walltime
        queue = manager.queue
        project = manager.project
        @info "Activating worker project $project"

        jobname = `julia-$(getpid())`

        Jcmd = np > 1 ? `-J 1-$np` : ``
        if isempty(project)
            project = dirname(Base.active_project())
        end

        script = ClusterManagers.worker_arg()
        julia_cmd = build_julia_command(; exename, exeflags, project, script, logfile,
                                        mem_sandbox)

        jobname = Base.shell_escape(jobname)

        cmd = """#!/bin/bash
        #PBS -N $jobname
        #PBS -V
        #PBS -j oe
        #PBS -m n
        #PBS -o $(LOGDIR)/$jobname.final.log
        #PBS $(Base.shell_escape(Jcmd))
        $(format_pbs_resources(ncpus, mem, walltime))
        cd $dir
        source $(ENV["HOME"])/.bashrc
        $(to_string(julia_cmd))
        """

        f = tempname(LOGDIR)
        write(f, cmd)

        @debug(cmd)
        if isempty(queue)
            _qsub = "/usr/physics/pbspro/bin/qsub"
        else
            _qsub = "/usr/physics/pbspro/bin/qsub -q $(Base.shell_escape(queue))"
        end
        qsub = "source $(ENV["HOME"])/.bashrc > /dev/null && $(Base.shell_escape(_qsub)) $(Base.shell_escape(f))"

        qsub_cmd = pipeline(`ssh headnode "$qsub"`)
        @debug qsub_cmd
        out = open(qsub_cmd)
        @debug out
        if !success(out)
            throw(error()) # qsub already gives a message
        end
        line = readline(out)
        id = chomp(split(line, '.')[1])
        @debug id
        if endswith(id, "[]")
            id = id[1:(end - 2)]
        end
        if isnothing(tryparse(Int, id))
            error("Job id could not be parse from worker output '$line'. Please make sure tour `.bashrc` file does not print anything to stdout.")
        end

        function filenames(i)
            if np > 1
                ["$LOGDIR/$id[$i].headnode.log"]
            else
                ["$LOGDIR/$id.headnode.log"]
            end
        end

        println("Job $id in queue.")
        for i in 1:np
            # wait for each output stream file to get created
            fnames = filenames(i)
            j = 0
            if haskey(ENV, "JULIA_WORKER_TIMEOUT")
                hosttimeout = tryparse(Int, ENV["JULIA_WORKER_TIMEOUT"])
            else
                hosttimeout = 480
            end
            start_time = time()
            while (j = findfirst(x -> isfile(x), fnames)) === nothing &&
                (time() - start_time) < hosttimeout
                sleep(0.5)
                @debug "Waiting for worker $i to connect at $fnames"
                @debug isfile(fnames[1])
            end
            (j = findfirst(x -> isfile(x), fnames)) === nothing &&
                error("Worker $i did not connect at $fnames after $hosttimeout seconds.")
            fname = fnames[j]

            # Hack to get Base to get the host:port, the Julia process has already started.
            # cmd = `tail -f $fname`
            host = readline(fname)
            start_time = time()
            while isempty(host) && (time() - start_time) < hosttimeout
                sleep(0.5)
                @debug "Waiting for worker $i to write hostname to $fname"
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
        println("Running. See stdout of children at $LOGDIR (jobid: $id)")

    catch e
        println("Error launching workers")
        println(e)
    end
end

function ClusterManagers.manage(manager::PBSProManager,
                                id::Int64, config::WorkerConfig, op::Symbol)
end

function ClusterManagers.kill(manager::PBSProManager, id::Int64, config::WorkerConfig)
    @info "Killing process $id"
    remotecall(exit, id)
    # close(config.io)
    ## pbsid = Distributed.map_pid_wrkr[id].config.userdata[:job]
    #pbsid = split(ENV["PBS_JOBID"], ".") |> first
    #run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
    #@info "Killed process $id, job $pbsid."

    # * Delete all tail commands associated with this job id
    # pids = run(`ps aux \| grep tail \| grep head`)
    # * Now delete any pids matching this pbs id

    # if isfile(config.userdata[:iofile])
    #     rm(config.userdata[:iofile])
    # end
end

function addprocs(np::Integer, ncpus, mem, walltime; qsub_flags = ``, project = ``,
                  kwargs...)
    ClusterManagers.addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags, project);
                             enable_threaded_blas = true, kwargs...)
end

function addprocs(np::Integer; ncpus = 10, mem = 31, walltime = 48, qsub_flags = ``,
                  project = ``,
                  kwargs...)
    ClusterManagers.addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags, project);
                             enable_threaded_blas = true, kwargs...)
end

function addprocs(f::Function; preamble = nothing, args, kwargs, _kwargs...)
    p = addprocs(1; _kwargs...) |> only
    if !isnothing(preamble)
        if !(preamble isa Expr)
            preamble = Meta.parse(preamble)
        end
        # @everywhere p $preamble
        o = @spawnat p eval(preamble)
        wait(o)
    end
    # function func(f, args...; kwargs...)
    #     try
    #         f(args...; kwargs...)
    #     catch e
    #         e
    #     end
    # end
    o = remotecall_fetch(f, p, args...; kwargs...)
    @info "Worker $p completed successfully, removing."
    # remotecall(exit, p)
    # close(Distributed.map_pid_wrkr[p].config.io)
    # if isfile(Distributed.map_pid_wrkr[p].config.userdata[:iofile])
    #     rm(Distributed.map_pid_wrkr[p].config.userdata[:iofile])
    # end
    pbsid = Distributed.map_pid_wrkr[p].config.userdata[:job]
    # @info pbsid
    run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
    @info "Worker $p removed successfully."
    return o
end
function addprocs(f::Function, itr; preamble = nothing, args = (), kwargs = (;), _kwargs...)
    O = Vector{Any}(undef, length(itr))
    procs = addprocs(length(itr); _kwargs...)
    if !isnothing(preamble)
        if !(preamble isa Expr)
            preamble = Meta.parse(preamble)
        end
        @everywhere procs $preamble
    end
    @sync for i in eachindex(itr)
        p = procs[i]
        # function func(f, args...; kwargs...)
        #     try
        #         f(args...; kwargs...)
        #     catch e
        #         e
        #     end
        # end
        o = @async remotecall_fetch(f, p, (itr[i], args...); kwargs...)
        O[i] = o
    end
    @info "Workers completed successfully, removing."
    for p in procs
        pbsid = Distributed.map_pid_wrkr[p].config.userdata[:job]
        run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
        @info "Job $pbsid removed successfully."
    end
    return O
end
function addprocs(f::Function, itr, batchsize::Integer; args = (), kwargs = (;),
                  _kwargs...)
    if batchsize == 1
        O = Vector{Any}(undef, length(itr))
        for i in eachindex(itr)
            o = @async addprocs(f; args = (itr[i], args...), kwargs = kwargs, _kwargs...)
            O[i] = o
        end
    else # This helps because precompilation always takes place on the calling process, so want to limit the number of times it happens, but still asynchronously start jobs
        batches = collect(Iterators.partition(eachindex(itr), batchsize))
        O = Vector{Any}(undef, length(batches))
        for bi in eachindex(batches)
            o = @async addprocs(f, itr[batches[bi]]; args, kwargs = kwargs, _kwargs...)
            O[bi] = o
        end
    end
    return O
end

function runscript(script::String;
                   ncpus = 10,
                   mem = 31,
                   walltime = 48,
                   qsub_flags = "",
                   project = ``,
                   exeflags = ``,
                   mem_sandbox = ceil(Int, mem * 1.25),
                   kwargs...)
    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}_$(ID).log`
    exeflags = `$exeflags --heap-size-hint=$(ceil(Int, mem/2))G`

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, mem_sandbox,
                                    kwargs...)

    cmd = """#!/bin/bash
    #PBS -N $(ID)
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
    qsub = "source $(ENV["HOME"])/.bashrc && /usr/physics/pbspro/bin/qsub $(string(qsub_flags)) $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    run(qsub_cmd)
    return nothing
end

function runscripts(exprs;
                    ncpus = 10,
                    mem = 31,
                    walltime = 48,
                    qsub_flags = "",
                    project = ``,
                    exeflags = ``,
                    mem_sandbox = ceil(Int, mem * 1.25),
                    kwargs...)
    uID = rand(UInt16) |> Int
    N = length(exprs)

    jobarray_script_prefix = `$(LOGDIR)/$(uID)`

    mkpath(expanduser(jobarray_script_prefix))
    scriptfiles = map(enumerate(exprs)) do (i, ex)
        file = expanduser("$(jobarray_script_prefix)/$i.jl")
        open(file, "w") do f
            write(f, string(ex))
        end
        return file
    end

    script = `$(jobarray_script_prefix)/\$\{PBS_ARRAY_INDEX\}.jl`
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}_$(ID).log`
    exeflags = `$exeflags --heap-size-hint=$(ceil(Int, mem/2))G`

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, mem_sandbox,
                                    kwargs...)
    cmd = """#!/bin/bash
    #PBS -N $(uID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$uID.final.log
    $(format_pbs_resources(ncpus, mem, walltime))
    #PBS -J 1-$N
    source $(ENV["HOME"])/.bashrc
    cd $project
    $(to_string(julia_cmd))
    """

    qsub_file = first(mktemp(LOGDIR; cleanup = false))
    open(qsub_file, "w") do f
        write(f, cmd)
    end
    qsub = "source $(ENV["HOME"])/.bashrc && /usr/physics/pbspro/bin/qsub $(string(qsub_flags)) $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    @info "Submitting array job with id $uID (logdir: $LOGDIR)"
    run(qsub_cmd)
    return nothing
end

function runscript(expr::Expr; kwargs...)
    file = first(mktemp(LOGDIR; cleanup = false))
    open(file, "w") do f
        write(f, string(expr))
    end
    runscript(file; kwargs...)
end

function selfdestruct()
    pbsid = split(ENV["PBS_JOBID"], ".") |> first
    @info "Nuking job $pbsid"
    run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
    @info "Nuked job $pbsid." # All going well, this won't run
end
end # module
