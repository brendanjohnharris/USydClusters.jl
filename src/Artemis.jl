
using ClusterManagers
import ClusterManagers.worker_arg
import ClusterManagers.ClusterManager
import ClusterManagers.WorkerConfig

export PBSProManager, addprocs_pbspro
struct PBSProManager <: ClusterManager
    np::Integer
    ncpus::Integer
    mem::Integer # GB
    walltime::Integer # Hours
    queue::Any
    prequel::Any
end
function PBSProManager(np, ncpus, mem, walltime, queue)
    PBSProManager(np, ncpus, mem, walltime, queue, ``)
end

function ClusterManagers.launch(manager::PBSProManager,
                                params::Dict, instances_arr::Array, c::Condition)
    try
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]
        # exeflags = `$exeflags`

        np = manager.np
        ncpus = manager.ncpus
        mem = manager.mem
        walltime = manager.walltime
        queue = manager.queue
        prequel = manager.prequel

        jobname = `julia-$(getpid())`

        # cmd = `cd $dir '&&' source /home/bhar9988/.bashrc '&&' module load gcc/12.1.0 '&&' export JULIA_WORKER_TIMEOUT="360" '&&' $exename --project=/home/bhar9988/code/AllenAttention.jl/ $exeflags $(worker_arg())` # 2'>''&'1 '|' tee '~'/jobs/julia_subprocess.log
        # cmd = `cd $dir '&&' source /home/bhar9988/.bashrc '&&' export JULIA_WORKER_TIMEOUT="360" '&&' julia -L '~'/.julia/config/startup.jl --worker=12345` # 2'>''&'1 '|' tee '~'/jobs/julia_subprocess.log
        Jcmd = np > 1 ? `-J 1-$np` : ()
        if isempty(prequel)
            prequel = `` # It's something
        end
        cmd = """$(Base.shell_escape(prequel))
        cd $dir
        source /home/bhar9988/.bashrc
        module unload gcc
        module unload pango
        module load gcc/12.1.0
        # gcc --version
        export JULIA_WORKER_TIMEOUT="360"
        export PYCALL_JL_RUNTIME_PYTHON="/home/bhar9988/code/AllenAttention.jl/.CondaPkg/env/bin/python3.9"
     export PYTHON="/home/bhar9988/code/AllenAttention.jl/.CondaPkg/env/bin/python3.9"
        export JULIA_CONDAPKG_OFFLINE="yes"
        export JULIA_PYTHONCALL_EXE="@PyCall"
        export JULIA_CONDAPKG_BACKEND="Null"
        $(Base.shell_escape(exename)) -t auto --project=/home/bhar9988/code/AllenAttention.jl/ $(Base.shell_escape(exeflags)) $(Base.shell_escape(ClusterManagers.worker_arg())) 2>&1 | tee ~/jobs/julia_subprocess.log"""
        f = tempname()
        write(f, cmd)
        # qsub_cmd = pipeline(`echo $(Base.shell_escape(cmd))`, `qsub -N $jobname -V -j oe -k o -m ae -M bhar9988@uni.sydney.edu.au $Jcmd -l select=1:ncpus=$(ncpus):mem=$(mem)GB -l walltime=$(walltime):00:00 $queue`)
        @debug(cmd)
        qsub_cmd = pipeline(`qsub -N $jobname -V -j oe -k o -m ae -M bhar9988@uni.sydney.edu.au $Jcmd -l select=1:ncpus=$(ncpus):mem=$(mem)GB -l walltime=$(walltime):00:00 $queue $f`)
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
                ["/home/bhar9988/julia-$(getpid()).o$id-$i",
                 "/home/bhar9988/julia-$(getpid())-$i.o$id",
                 "/home/bhar9988/julia-$(getpid()).o$id.$i"]
            else
                ["/home/bhar9988/julia-$(getpid()).o$id"]
            end
        end

        println("Job $id in queue.")
        for i in 1:np
            # wait for each output stream file to get created
            fnames = filenames(i)
            j = 0
            while (j = findfirst(x -> isfile(x), fnames)) == nothing
                sleep(1.0)
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

function addprocs_pbspro(np::Integer, ncpus, mem, walltime; qsub_flags = ``, kwargs...)
    addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags); kwargs...)
end

function addprocs_pbspro(np::Integer; ncpus = 8, mem = 96, walltime = 96, qsub_flags = ``,
                         kwargs...)
    addprocs(PBSProManager(np, ncpus, mem, walltime, qsub_flags); kwargs...)
end
