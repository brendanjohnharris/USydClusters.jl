
module Physics
using Distributed
using ClusterManagers
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
        home = ENV["HOME"]
        jobdir = home * "/jobs"
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]
        # exeflags = `$exeflags`

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
        cmd = """#!/bin/bash
        #PBS -N $(Base.shell_escape(jobname))
        #PBS -V
        #PBS -j oe
        #PBS -m ae
        #PBS -o $(jobdir)/$(Base.shell_escape(jobname)).final.log
        #PBS -M bhar9988@uni.sydney.edu.au
        #PBS $(Base.shell_escape(Jcmd))
        #PBS -l select=1:ncpus=$((ncpus)):mem=$(mem)GB
        #PBS -l walltime=$((walltime)):00:00 $(Base.shell_escape(queue))
        cd $dir
        source /headnode2/bhar9988/.bashrc
        $(Base.shell_escape(exename)) -t auto --heap-size-hint=$(mem÷2)G --project=$project $(Base.shell_escape(exeflags)) $(Base.shell_escape(ClusterManagers.worker_arg())) 2>&1 | tee ~/jobs/\${PBS_JOBID}.log"""
        f = tempname(jobdir)
        write(f, cmd)
        # qsub_cmd = pipeline(`echo $(Base.shell_escape(cmd))`, `qsub -N $jobname -V -j oe -k o -m ae -M bhar9988@uni.sydney.edu.au $Jcmd -l select=1:ncpus=$(ncpus):mem=$(mem)GB -l walltime=$(walltime):00:00 $queue`)
        @debug(cmd)
        mkpath(jobdir)
        qsub = "source ~/.tcshrc && /usr/physics/pbspro/bin/qsub $(Base.shell_escape(f))"
        qsub_cmd = pipeline(`ssh headnode "$qsub"`)
        @debug qsub_cmd
        out = open(qsub_cmd)
        @debug out
        if !success(out)
            throw(error()) # qsub already gives a message
        end

        id = chomp(split(readline(out), '.')[1])
        @debug id
        if endswith(id, "[]")
            id = id[1:(end - 2)]
        end

        function filenames(i)
            if np > 1
                ["$jobdir/$id[$i].headnode.log"]
            else
                ["$jobdir/$id.headnode.log"]
            end
        end

        println("Job $id in queue.")
        for i in 1:np
            # wait for each output stream file to get created
            fnames = filenames(i)
            j = 0
            if haskey(ENV, "JULIA_WORKER_TIMEOUT")
                hosttimeout = ENV["JULIA_WORKER_TIMEOUT"]
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
        println("Running. See stdout of children at $jobdir (jobid: $id)")

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
    @info "Workerscompleted successfully, removing."
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

function runscript(file::String; parent = "~/jobs/", ncpus = 10, mem = 31, walltime = 48,
                   qsub_flags = "", project = ``, exename = `julia`,
                   exeflags = ``,
                   kwargs...)
    ID = file |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    cmd = """#!/bin/bash
    #PBS -N $(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m ae
    #PBS -o ~/jobs/$(ID).final.log
    #PBS -M bhar9988@uni.sydney.edu.au
    #PBS -l select=1:ncpus=$((ncpus)):mem=$(mem)GB
    #PBS -l walltime=$((walltime)):00:00
    source /headnode2/bhar9988/.bashrc
    cd $project
    $(Base.shell_escape(exename)) $(Base.shell_escape(exeflags)) -t auto --heap-size-hint=$(mem÷2)G --project=$project $(Base.shell_escape(file)) 2>&1 | tee ~/jobs/$(ID).log"""
    qsub_file = first(mktemp(parent; cleanup = false))
    open(qsub_file, "w") do f
        write(f, cmd)
    end
    qsub = "source ~/.tcshrc && /usr/physics/pbspro/bin/qsub $(string(qsub_flags)) $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    run(qsub_cmd)
    return nothing
end

function runscript(expr::Expr; parent = ENV["HOME"] * "/jobs/", kwargs...)
    file = first(mktemp(parent, ; cleanup = false))
    open(file, "w") do f
        write(f, string(expr))
    end
    runscript(file; parent, kwargs...)
end # Have one for vector of exprs, job arrays?

function selfdestruct()
    pbsid = split(ENV["PBS_JOBID"], ".") |> first
    @info "Nuking job $pbsid"
    run(`ssh headnode "/usr/physics/pbspro/bin/qdel $pbsid"`)
    @info "Nuked job $pbsid." # All going well, this won't run
end
end # module
