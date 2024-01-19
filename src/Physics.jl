
module Physics
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

        jobname = `julia-$(getpid())`

        Jcmd = np > 1 ? `-J 1-$np` : ``
        if isempty(project)
            project = "/headnode2/bhar9988/code/DDC/AllenAttention.jl/" # `@.`
        end
        cmd = """#!/bin/bash
        #PBS -N $(Base.shell_escape(jobname))
        #PBS -V
        #PBS -j oe
        #PBS -m ae
        #PBS -M bhar9988@uni.sydney.edu.au
        #PBS $(Base.shell_escape(Jcmd))
        #PBS -l select=1:ncpus=$((ncpus)):mem=$(mem)GB
        #PBS -l walltime=$((walltime)):00:00 $(Base.shell_escape(queue))
        cd $dir
        source /headnode2/bhar9988/.bashrc
        export JULIA_WORKER_TIMEOUT=360
        $(Base.shell_escape(exename)) -t auto --project=$project $(Base.shell_escape(exeflags)) $(Base.shell_escape(ClusterManagers.worker_arg())) 2>&1 | tee ~/jobs/\${PBS_JOBID}.log"""
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
            while (j = findfirst(x -> isfile(x), fnames)) === nothing
                sleep(1.0)
                @debug "Waiting for worker $i to connect at $fnames"
                @debug isfile(fnames[1])
            end
            fname = fnames[j]

            # Hack to get Base to get the host:port, the Julia process has already started.
            cmd = `tail -f $fname`

            config = WorkerConfig()

            config.io = open(detach(cmd))

            config.userdata = Dict{Symbol, Any}(:job => id, :task => i, :iofile => fname)
            push!(instances_arr, config)
            notify(c)
        end
        rm(f, force = true)
        println("Running.")

    catch e
        println("Error launching workers")
        println(e)
    end
end

function ClusterManagers.manage(manager::PBSProManager,
                                id::Int64, config::WorkerConfig, op::Symbol)
end

function ClusterManagers.kill(manager::PBSProManager, id::Int64, config::WorkerConfig)
    remotecall(exit, id)
    close(config.io)

    if isfile(config.userdata[:iofile])
        rm(config.userdata[:iofile])
    end
end

function addprocs(np::Integer, ncpus, mem, walltime; qsub_flags = ``, kwargs...)
    ClusterManagers.addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags);
                             enable_threaded_blas = true, kwargs...)
end

function addprocs(np::Integer; ncpus = 10, mem = 31, walltime = 48, qsub_flags = ``,
                  kwargs...)
    ClusterManagers.addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags);
                             enable_threaded_blas = true, kwargs...)
end
end # module
