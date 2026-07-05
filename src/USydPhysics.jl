module USydPhysics
using Distributed
import AcademicClusters: build_julia_command, LOGDIR, to_string

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
    worker_cookie() -> String

Generate a cookie for Distributed worker authentication.
"""
function worker_cookie()
    Distributed.init_multi()
    return Distributed.cluster_cookie()
end

"""
    worker_arg() -> Cmd

Generate the command-line argument for starting a Julia worker process.
"""
worker_arg() = `--worker=$(worker_cookie())`

# ===========================
# PBS Pro Manager
# ===========================

export PBSProManager, addprocs, runscript, runscripts, selfdestruct

"""
    PBSProManager <: ClusterManager

A cluster manager for launching Julia workers on PBS Pro job scheduler.

# Fields
- `np::Integer`: Number of parallel workers to launch
- `ncpus::Integer`: Number of CPUs per worker
- `ngpus::Integer`: Number of GPUs per worker (default 0)
- `mem::String`: Memory per worker with units (e.g., "16GB", "2048MB")
- `walltime::String`: Maximum walltime in HH:MM:SS format
- `queue::Cmd`: PBS queue name (optional)
- `project::Cmd`: Project directory (optional)
- `qsubflags::Cmd`: Additional qsub flags (optional)
"""
struct PBSProManager <: ClusterManager
    np::Int
    ncpus::Int
    ngpus::Int
    mem::String  # memory with units
    walltime::String  # HH:MM:SS format
    queue::Cmd
    project::Cmd
    qsubflags::Cmd
end

"""
    PBSProManager(np=1; ncpus=8, ngpus=0, mem=16, walltime=24, queue=``, project=``, qsubflags=``)

Create a PBS Pro cluster manager.

# Arguments
- `np::Integer=1`: Number of parallel workers to launch
- `ncpus::Integer=8`: Number of CPUs per worker
- `ngpus::Integer=0`: Number of GPUs per worker
- `mem::Union{Real,String}=16`: Memory per worker (number as GB or string with units)
- `walltime::Union{Integer,String}=24`: Maximum walltime (hours or "HH:MM:SS")
- `queue::Cmd=```: PBS queue name
- `project::Cmd=```: Project directory
- `qsubflags::Cmd=```: Additional qsub flags

# Examples
```julia
PBSProManager(4; mem=32, walltime=24)         # 32GB, 24 hours
PBSProManager(4; mem="2048MB", walltime="12:30:00")
PBSProManager(4; mem=0.5, walltime=2)         # 512MB, 2 hours
PBSProManager(4; ngpus=1, mem=32)             # 1 GPU per worker
```
"""
function PBSProManager(
        np::Integer = 1;
        ncpus::Integer = 8,
        ngpus::Integer = 0,
        mem::Union{Real, AbstractString} = 16,
        walltime::Union{Integer, AbstractString} = 24,
        queue::Cmd = ``,
        project::Cmd = ``,
        qsubflags::Cmd = ``
    )
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    np > 0 || throw(ArgumentError("np must be a positive integer, got $np"))
    ncpus > 0 || throw(ArgumentError("ncpus must be a positive integer, got $ncpus"))
    ngpus >= 0 || throw(ArgumentError("ngpus must be a non-negative integer, got $ngpus"))

    return PBSProManager(np, ncpus, ngpus, mem_str, walltime_str, queue, project, qsubflags)
end

"""
No-op for PBS Pro.
"""
function Distributed.manage(
        manager::PBSProManager,
        id::Int64, config::WorkerConfig, op::Symbol
    )
end

"""
    Distributed.launch(manager::PBSProManager, params::Dict, instances_arr::Array, c::Condition)

Launch PBS jobs for distributed workers.

# Arguments
- `manager`: PBS Pro manager instance
- `params`: Launch parameters from Distributed.jl
- `instances_arr`: Array to populate with worker configurations
- `c`: Condition variable for synchronization

# Returns
- Job ID string on success, `false` on failure
"""
function Distributed.launch(
        manager::PBSProManager,
        params::Dict, instances_arr::Array, c::Condition
    )
    try
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]

        np = manager.np
        ncpus = manager.ncpus
        ngpus = manager.ngpus
        mem = manager.mem
        walltime = manager.walltime

        queue = manager.queue
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

        # Setup job array and logging; np > 0 guaranteed by the constructor
        if np == 1
            Jcmd = ""
            logdir = `$(LOGDIR)`
            logfile = `$(to_string(logdir))/\$\{MAIN_JOBID\}.$(ID).log`
        else
            Jcmd = "#PBS -J 1-$np"
            logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
            logfile = `$(to_string(logdir))/\$\{PBS_ARRAY_INDEX\}.log`
        end

        script = worker_arg()
        exeflags = with_heap_hint(exeflags, mem)
        julia_cmd = build_julia_command(; exename, exeflags, project, script, logfile)

        ID = Base.shell_escape("$(ID)")
        gpu_resource = ngpus > 0 ? ",ngpus=$(ngpus)" : ""
        cmd = """#!/bin/bash
        #PBS -N julia-$ID
        #PBS -V
        #PBS -j oe
        #PBS -m n
        #PBS -o $(LOGDIR)/$ID.final.log
        $(Jcmd)
        #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem)),walltime=$(walltime)$(gpu_resource)
        cd $dir
        source $(homedir())/.bashrc
        MAIN_JOBID=\${PBS_JOBID%\\[*}
        MAIN_JOBID=\${MAIN_JOBID%%.*}
        mkdir -p "$(to_string(logdir))"
        $(to_string(julia_cmd))
        """
        @debug cmd

        f = tempname(LOGDIR)
        write(f, cmd)

        _qsub = `/opt/pbs/bin/qsub $(queue) $(qsubflags)`
        qsub = "source $(homedir())/.bashrc > /dev/null 2>&1 && $(Base.shell_escape(_qsub)) $(Base.shell_escape(f))"
        qsub_cmd = pipeline(`ssh headnode "$qsub"`, stderr = devnull)

        @debug "Submitting PBS job: $qsub_cmd"

        output = ""
        try
            output = read(qsub_cmd, String)
        catch e
            rm(f, force = true)
            error("Failed to submit PBS job: $e")
        end
        rm(f, force = true)

        output = strip(output)
        if isempty(output)
            error("PBS submission returned empty output. Check queue availability and permissions.")
        end

        # Extract job ID (format: JOBID.hostname or JOBID[].hostname for arrays)
        job_parts = split(output, '.')
        if length(job_parts) < 2
            error("Unexpected PBS output format: '$output'. Expected 'JOBID.hostname'")
        end

        id = first(job_parts) |> chomp
        id = replace(id, r"\[\]" => "")

        @debug "Parsed job ID: $id"

        # Reconstruct log file names
        if np == 1
            logfile = `$(to_string(logdir))/$id.$(ID).log`
            fnames = ["$(to_string(logfile))"]
        else
            logdir = `$(LOGDIR)/$id\[\].$(ID).log`
            fnames = ["$(to_string(logdir))/$i.log" for i in 1:np]
        end

        # Validate job ID
        if isnothing(tryparse(Int, id))
            error("Job id could not be parsed from PBS output '$output'. Expected numeric job ID, got '$id'. Please check your `.bashrc` file doesn't print to stdout.")
        end

        println("Job $id submitted to queue.")

        # Wait for workers to connect
        hosttimeout = get(ENV, "JULIA_WORKER_TIMEOUT", "480") |> x -> tryparse(Int, x)
        hosttimeout = something(hosttimeout, 480)

        # Track SSH tunnels for cleanup on failure
        tunnels_to_cleanup = Base.Process[]

        for i in 1:np
            try
                fname = fnames[i]

                # Wait for log file creation with exponential backoff
                start_time = time()
                wait_interval = 0.5
                while !isfile(fname) && (time() - start_time) < hosttimeout
                    sleep(wait_interval)
                    wait_interval = min(wait_interval * 1.5, 5.0)  # Cap at 5 seconds
                end

                if !isfile(fname)
                    error("Worker $i did not create log file at $fname after $hosttimeout seconds.")
                end

                # Read host:port from log file with retries
                host_info = ""
                start_time = time()
                retry_count = 0
                max_retries = 10

                while (time() - start_time) < hosttimeout
                    try
                        # Open file in read mode to avoid race conditions
                        open(fname, "r") do f
                            host_info = readline(f)
                        end

                        # Check if we got valid data
                        if !isempty(host_info) && occursin('#', host_info) &&
                                occursin(':', host_info)
                            break
                        end
                    catch e
                        @debug "Error reading host info from $fname: $e"
                    end

                    retry_count += 1
                    if retry_count > max_retries
                        @warn "Exceeded max retries reading from $fname, waiting..."
                    end
                    sleep(0.5)
                end

                if isempty(host_info)
                    error("Worker $i: Hostname not written to file after $hosttimeout seconds.")
                end

                # Parse host:port with error handling
                host_parts = split(host_info, ['#', ':'])
                if length(host_parts) < 3
                    error("Worker $i: Invalid host info format: '$host_info'. Expected format: 'prefix#port:hostname'")
                end

                port = tryparse(Int, host_parts[2])
                if isnothing(port)
                    error("Worker $i: Invalid port number: '$(host_parts[2])'")
                end

                host = host_parts[3]

                config = WorkerConfig()

                # Configure connection (direct or via SSH tunnel)
                if isdir("/opt/pbs")
                    # Direct connection from cluster node
                    config.host = host
                    config.port = port
                    config.userdata = Dict{Symbol, Any}(
                        :job => id, :task => i,
                        :iofile => fname
                    )
                else
                    # Setup SSH tunnel with retry logic for port conflicts
                    tunnel_established = false
                    tunnel_proc = nothing
                    max_port_attempts = 10

                    for attempt in 1:max_port_attempts
                        local_port = rand(10000:60000)

                        # ControlPath=none/ControlMaster=no: never multiplex. These are many
                        # concurrent, forward-only sessions; sharing a user ControlMaster master
                        # (e.g. `Host headnode` with ControlPersist) makes the slaves race and die.
                        tunnel_cmd = `ssh -4 -N -L 127.0.0.1:$local_port:$host:$port -o ControlPath=none -o ControlMaster=no -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 headnode`

                        @info "Worker $i: Setting up SSH tunnel (attempt $attempt/$max_port_attempts) on port $local_port for worker at $host:$port"

                        try
                            tunnel_proc = run(tunnel_cmd, wait = false)
                            push!(tunnels_to_cleanup, tunnel_proc)

                            # Check if tunnel is actually working by waiting a bit and checking process
                            sleep(1)
                            if !Base.process_running(tunnel_proc)
                                sleep(3)
                            end
                            if Base.process_running(tunnel_proc)
                                tunnel_established = true
                                config.host = "127.0.0.1"
                                config.port = local_port
                                config.userdata = Dict{Symbol, Any}(
                                    :job => id,
                                    :task => i,
                                    :iofile => fname,
                                    :tunnel => tunnel_proc
                                )
                                @info "Success"
                                break
                            else
                                @warn "Worker $i: SSH tunnel died shortly after creation"
                            end
                        catch e
                            @warn "Worker $i: Failed to establish SSH tunnel on port $local_port: $e"
                            if tunnel_proc !== nothing && Base.process_running(tunnel_proc)
                                kill(tunnel_proc)
                            end
                        end
                    end

                    if !tunnel_established
                        error("Worker $i: Failed to establish SSH tunnel after $max_port_attempts attempts")
                    end
                end

                push!(instances_arr, config)
                notify(c)

            catch e
                # Clean up any established tunnels before re-throwing
                for tunnel in tunnels_to_cleanup
                    try
                        if Base.process_running(tunnel)
                            kill(tunnel)
                        end
                    catch
                        # Ignore errors during cleanup
                    end
                end
                rethrow(e)
            end
        end

        logloc = np == 1 ? logfile : logdir
        println("Running. See stdout of children at $logloc")
        return id

    catch e
        println("Error launching workers: $e")
        @error "Full error details" exception = (e, catch_backtrace())

        # Clean up temp file if it exists
        @isdefined(f) && isfile(f) && rm(f, force = true)

        # Clean up any SSH tunnels that were created
        if @isdefined(tunnels_to_cleanup) && !isempty(tunnels_to_cleanup)
            @info "Cleaning up $(length(tunnels_to_cleanup)) SSH tunnels"
            for tunnel in tunnels_to_cleanup
                try
                    if Base.process_running(tunnel)
                        kill(tunnel)
                    end
                catch
                    # Ignore errors during cleanup
                end
            end
        end

        return false
    end
end

"""
    Distributed.kill(manager::PBSProManager, id::Int64, config::WorkerConfig)

Kill a distributed worker process and clean up associated resources.

# Arguments
- `manager`: PBS Pro manager instance
- `id`: Worker process ID
- `config`: Worker configuration
"""
function Distributed.kill(manager::PBSProManager, id::Int64, config::WorkerConfig)
    @debug "Killing worker process $id"

    # First try to gracefully exit the worker
    try
        remotecall(exit, id)
    catch e
        @debug "Failed to send exit command to worker $id: $e"
    end

    # Clean up SSH tunnel if it exists
    if haskey(config.userdata, :tunnel)
        tunnel = config.userdata[:tunnel]
        try
            if Base.process_running(tunnel)
                @debug "Killing SSH tunnel for worker $id"
                kill(tunnel)
            end
        catch e
            @debug "Error killing SSH tunnel: $e"
        end
    end

    # If we have the PBS job ID, we could also qdel it
    return if haskey(config.userdata, :job)
        job_id = config.userdata[:job]
        task = get(config.userdata, :task, 1)
        @debug "PBS job $job_id (task $task) cleanup completed"
    end
end

"""
    addprocs(np::Integer; kwargs...) -> Vector{Int}

Add distributed workers using PBS Pro.

# Arguments
- `np::Integer`: Number of workers to add
- `ncpus::Integer=8`: CPUs per worker
- `ngpus::Integer=0`: GPUs per worker
- `mem::Union{Real,String}=16`: Memory per worker (GB as number or string with units)
- `walltime::Union{Integer,String}=24`: Maximum runtime (hours or "HH:MM:SS")
- `queue::Cmd=```: PBS queue name
- `project::Cmd=```: Project directory
- `qsubflags::Cmd=```: Additional qsub flags

# Returns
- Vector of worker process IDs

# Examples
```julia
# Add 4 workers with 16GB RAM each, 24 hour walltime
addprocs(4; mem=16, walltime=24)
addprocs(4; mem="16GB", walltime="24:00:00")

# Add 2 workers with specific resources
addprocs(2; ncpus=16, mem=0.5, walltime=2)  # 512MB, 2 hours
addprocs(2; ncpus=16, mem="32GB", walltime="02:30:00")

# Add 4 workers with 1 GPU each
addprocs(4; ngpus=1, mem=32)
```
"""
function addprocs(
        np::Integer;
        ncpus::Integer = 8,
        ngpus::Integer = 0,
        mem::Union{Real, AbstractString} = 16,
        walltime::Union{Integer, AbstractString} = 24,
        queue::Cmd = ``,
        project::Cmd = ``,
        qsubflags::Cmd = ``,
        kwargs...
    )
    return Distributed.addprocs(
        PBSProManager(
            np; ncpus, ngpus, mem, walltime, queue, project,
            qsubflags
        );
        enable_threaded_blas = true,
        kwargs...
    )
end

# ===========================
# Script Execution Functions
# ===========================

"""
    capture_jobid(cmd::Cmd) -> (String, String)

Run a qsub command and parse (jobid, server) from its 'JOBID.server' output.
Array-job brackets are stripped from the ID, matching `Distributed.launch`.
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

"""
    runscript(script::String; kwargs...) -> (String, String)

Submit a Julia script as a PBS job.

# Arguments
- `script::String`: Path to Julia script file
- `ncpus::Integer=10`: Number of CPUs
- `mem::Union{Real,String}=31`: Memory (number as GB or string with units)
- `walltime::Union{Integer,String}=48`: Walltime (hours or "HH:MM:SS")
- `qsubflags::Cmd=```: Additional qsub flags
- `project::Cmd=```: Project directory
- `exeflags::Cmd=```: Julia executable flags
- `queue::Cmd=```: PBS queue

# Returns
- Tuple of (job_id, logfile_path)

# Examples
```julia
jobid, logfile = runscript("myscript.jl"; mem=64, walltime=12)
jobid, logfile = runscript("myscript.jl"; mem="64GB", walltime="12:00:00")
```
"""
function runscript(
        script::String;
        ncpus::Integer = 10,
        mem::Union{Real, AbstractString} = 31,
        walltime::Union{Integer, AbstractString} = 48,
        qsubflags::Cmd = ``,
        project::Cmd = ``,
        exeflags::Cmd = ``,
        queue::Cmd = ``,
        kwargs...
    )
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}.$(ID).log`
    exeflags = with_heap_hint(exeflags, mem_str)

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)

    cmd = """#!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$ID.final.log
    #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem_str)),walltime=$(walltime_str)
    source $(homedir())/.bashrc
    cd $(to_string(project))
    $(to_string(julia_cmd))
    """

    qsub_file = first(mktemp(LOGDIR; cleanup = false))
    write(qsub_file, cmd)

    queue = isempty(queue) ? queue : "-q $(Base.shell_escape(queue))"
    qsub = "source $(homedir())/.bashrc > /dev/null 2>&1 && /opt/pbs/bin/qsub $(to_string(qsubflags)) $queue $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    jobid, server = capture_jobid(qsub_cmd)

    return jobid, replace(to_string(logfile), r"\$\{PBS_JOBID\}" => "$jobid.$server")
end

"""
    runscript(expr::Expr; kwargs...) -> (String, String)

Submit a Julia expression as a PBS job.

# Arguments
- `expr::Expr`: Julia expression to execute
- kwargs: Same as `runscript(::String)`

# Returns
- Tuple of (job_id, logfile_path)
"""
function runscript(expr::Expr; kwargs...)
    file = first(mktemp(LOGDIR; cleanup = false))
    write_exprs(file, expr)
    return runscript(file; kwargs...)
end

"""
    runscripts(exprs::Vector; kwargs...) -> String

Submit multiple Julia expressions as a PBS array job.

# Arguments
- `exprs::Vector`: Vector of Julia expressions
- kwargs: Same as `runscript(::String)`

# Returns
- Job ID string
"""
function runscripts(exprs::Vector; kwargs...)
    if length(exprs) == 1
        runscript(exprs[1]; kwargs...)
    else
        ID = "runscripts_$(next_runscripts_id())"

        scriptdir = "$(LOGDIR)/$(ID).script"
        mkpath(expanduser(scriptdir))

        scriptfiles = map(enumerate(exprs)) do (i, ex)
            file = expanduser("$(scriptdir)/$i.jl")
            write_exprs(file, ex)
            return file
        end

        return runscripts(scriptdir; ID, kwargs...)
    end
end

"""
    runscripts(scriptdir::String; kwargs...) -> String

Submit multiple Julia scripts as a PBS array job.

# Arguments
- `scriptdir::String`: Directory containing numbered Julia scripts (1.jl, 2.jl, etc.)
- `ncpus::Integer=10`: Number of CPUs per job
- `mem::Union{Real,String}=31`: Memory per job (number as GB or string with units)
- `walltime::Union{Integer,String}=48`: Walltime per job (hours or "HH:MM:SS")
- `qsubflags::Cmd=```: Additional qsub flags
- `project::Cmd=```: Project directory
- `exeflags::Cmd=```: Julia executable flags
- `queue::Cmd=```: PBS queue
- `ID`: Job name identifier

# Returns
- Job ID string

# Examples
```julia
# Submit all scripts in directory as array job
jobid = runscripts("/path/to/scripts"; mem=32, walltime=6)
jobid = runscripts("/path/to/scripts"; mem="32GB", walltime="06:00:00")
```
"""
function runscripts(
        scriptdir::String;
        ncpus::Integer = 10,
        mem::Union{Real, AbstractString} = 31,
        walltime::Union{Integer, AbstractString} = 48,
        qsubflags::Cmd = ``,
        project::Cmd = ``,
        exeflags::Cmd = ``,
        queue::Cmd = ``,
        ID = "runscripts_$(next_runscripts_id())",
        kwargs...
    )
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    script = `$(scriptdir)/\$\{PBS_ARRAY_INDEX\}.jl`
    logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
    logfile = `$(to_string(logdir))/\$\{PBS_ARRAY_INDEX\}.log`
    exeflags = with_heap_hint(exeflags, mem_str)
    N = count(f -> occursin(r"^\d+\.jl$", f), readdir(scriptdir)) # only numbered scripts; ignores stray files

    N > 0 || throw(ArgumentError("No scripts found in directory: $scriptdir"))

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)

    cmd = """#!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$ID.final.log
    #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem_str)),walltime=$(walltime_str)
    #PBS -J 1-$N
    source $(homedir())/.bashrc
    cd $(to_string(project))
    MAIN_JOBID=\${PBS_JOBID%\\[*}
    MAIN_JOBID=\${MAIN_JOBID%%.*}
    mkdir -p "$(to_string(logdir))"
    $(to_string(julia_cmd))
    """

    qsub_file = first(mktemp(scriptdir; cleanup = false))
    write(qsub_file, cmd)

    queue = isempty(queue) ? queue : "-q $(Base.shell_escape(queue))"
    qsub = "source $(homedir())/.bashrc > /dev/null 2>&1 && /opt/pbs/bin/qsub $(to_string(qsubflags)) $queue $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`

    @info "Submitting array job with name julia-$ID (logdir: $LOGDIR)"
    jobid, _ = capture_jobid(qsub_cmd)

    return jobid
end

# ===========================
# Heterogeneous distribution (PBS queues + shared HPCs)
# ===========================

export distributeprocs

# PBS reports sizes like "63437mb", "58720256kb", "0b"
function pbs_size_gb(s::AbstractString)
    m = match(r"^(\d+(?:\.\d+)?)([kmgt]?)b$"i, strip(s))
    isnothing(m) && return 0.0
    factor = Dict(
        "" => 1 / 1024^3, "k" => 1 / 1024^2, "m" => 1 / 1024, "g" => 1.0,
        "t" => 1024.0
    )
    return parse(Float64, m[1]) * factor[lowercase(m[2])]
end

# Fields of a `key = value` block, keyed by exact prefix (pbsnodes -av, qstat -Qf)
function block_field(lines, key)
    i = findfirst(l -> startswith(strip(l), key * " = "), lines)
    return isnothing(i) ? nothing : strip(last(split(lines[i], " = ", limit = 2)))
end

"""
    parse_pbsnodes(text) -> Vector{NamedTuple}

Parse `pbsnodes -av` output into per-vnode (name, state, qlist, freecpus, freegb).
Parent hosts report zero resources; capacity lives on child vnodes like `nodegpu02[1]`.
"""
function parse_pbsnodes(text::AbstractString)
    vnodes = map(split(text, r"\n(?=\S)")) do block
        lines = split(block, '\n')
        name = strip(first(lines))
        isempty(name) && return nothing
        f(key) = block_field(lines, key)
        avail_cpus = something(tryparse(Int, something(f("resources_available.ncpus"), "")), 0)
        used_cpus = something(tryparse(Int, something(f("resources_assigned.ncpus"), "")), 0)
        avail_gb = pbs_size_gb(something(f("resources_available.mem"), ""))
        used_gb = pbs_size_gb(something(f("resources_assigned.mem"), ""))
        qlist = lowercase.(
            split(
                something(f("resources_available.Qlist"), ""), ',';
                keepempty = false
            )
        )
        return (;
            name, state = something(f("state"), "unknown"), qlist,
            freecpus = max(0, avail_cpus - used_cpus),
            freegb = max(0.0, avail_gb - used_gb),
        )
    end
    return [v for v in vnodes if !isnothing(v)]
end

"""
    parse_queues(text) -> Dict

Parse `qstat -Q -f` output into queue => (type, dest, ncpus_cap), where `ncpus_cap`
is the generic per-user running-ncpus limit (`max_run_res.ncpus = [u:PBS_GENERIC=N]`).
"""
function parse_queues(text::AbstractString)
    queues = Dict{String, NamedTuple}()
    for block in split(text, r"\n(?=Queue: )")
        lines = split(block, '\n')
        m = match(r"^\s*Queue: (\S+)", first(lines))
        isnothing(m) && continue
        cap = block_field(lines, "max_run_res.ncpus")
        capm = isnothing(cap) ? nothing : match(r"PBS_GENERIC=(\d+)", cap)
        queues[lowercase(m[1])] = (
            type = something(block_field(lines, "queue_type"), ""),
            dest = block_field(lines, "route_destinations"),
            ncpus_cap = isnothing(capm) ? nothing :
                parse(Int, capm[1]),
        )
    end
    return queues
end

# Running ncpus per queue from `qstat -u <user>` (TSK column of jobs in state R)
function parse_user_ncpus(text::AbstractString)
    usage = Dict{String, Int}()
    for l in split(text, '\n')
        startswith(l, r"\d") || continue
        p = split(l)
        length(p) >= 10 || continue
        tsk = tryparse(Int, p[7])
        (isnothing(tsk) || p[10] != "R") && continue
        q = lowercase(p[3])
        usage[q] = get(usage, q, 0) + tsk
    end
    return usage
end

function probe_cluster()
    remote = "pbsnodes -av; echo ===Q===; qstat -Q -f; echo ===U===; qstat -u $(ENV["USER"])"
    return try
        read(
            pipeline(
                `ssh -o BatchMode=yes -o ConnectTimeout=10 headnode "$remote"`,
                stderr = devnull
            ), String
        )
    catch
        ""
    end
end

"""
    cluster_capacities(text, queues, ncpus, mem_gb) -> Vector{Pair{String, Int}}

Workers of shape `ncpus` cores x `mem_gb` GB that can start immediately on each queue,
from a combined `pbsnodes -av`/`qstat -Q -f`/`qstat -u` probe. Routing queues (e.g.
defaultQ) are followed one hop to their execution queue for node matching and per-user
ncpus limits.
"""
function cluster_capacities(text::AbstractString, queues, ncpus, mem_gb)
    parts = split(text, r"===[QU]===")
    length(parts) == 3 || return [q => 0 for q in queues]
    vnodes = parse_pbsnodes(parts[1])
    qinfo = parse_queues(parts[2])
    usage = parse_user_ncpus(parts[3])
    return map(queues) do q
        info = get(qinfo, lowercase(q), nothing)
        eff = if !isnothing(info) && info.type == "Route" && !isnothing(info.dest)
            lowercase(first(split(info.dest, ',')))
        else
            lowercase(q)
        end
        fit = sum(vnodes; init = 0) do v
            v.state in ("free", "job-busy") && eff in v.qlist || return 0
            return min(v.freecpus ÷ ncpus, floor(Int, v.freegb / mem_gb))
        end
        einfo = get(qinfo, eff, nothing)
        if !isnothing(einfo) && !isnothing(einfo.ncpus_cap)
            remaining = max(0, einfo.ncpus_cap - get(usage, eff, 0))
            fit = min(fit, remaining ÷ ncpus)
        end
        return q => max(fit, 0)
    end
end
function cluster_capacities(queues, ncpus, mem_gb)
    return cluster_capacities(probe_cluster(), queues, ncpus, mem_gb)
end

# Our live HPC workers per host: loadavg lags freshly spawned (still-idle) workers, so
# repeated distributeprocs calls would double-book without explicit accounting
const HPC_WORKERS = Dict{String, Vector{@NamedTuple{pid::Int, cpus::Int, gb::Float64}}}()

# Prune dead pids and return (reserved cores, reserved GB) we already occupy on host
function reserved_on(host)
    ours = get(HPC_WORKERS, host, nothing)
    isnothing(ours) && return 0, 0.0
    filter!(w -> w.pid in Distributed.procs(), ours)
    return sum(w -> w.cpus, ours; init = 0), sum(w -> w.gb, ours; init = 0.0)
end

"""
    parse_hpc_capacity(text, ncpus, mem_gb, saturation; reserved_cores = 0, reserved_gb = 0) -> Int

Workers of shape `ncpus` x `mem_gb` that fit politely on a shared machine, from probe
output `nproc; cat /proc/loadavg; free -g` (available-memory column). `saturation`
caps the fraction of total cores and available memory we are willing to occupy;
`reserved_*` subtract resources our existing workers already claim (conservative: a
busy worker is also partly counted in loadavg).
"""
function parse_hpc_capacity(
        text::AbstractString, ncpus, mem_gb, saturation;
        reserved_cores = 0, reserved_gb = 0.0
    )
    lines = split(strip(text), '\n')
    length(lines) >= 3 || return 0
    cores = tryparse(Int, strip(lines[1]))
    load = tryparse(Float64, first(split(lines[2])))
    memparts = split(lines[3])
    avail_gb = length(memparts) >= 7 ? tryparse(Float64, memparts[7]) : nothing
    any(isnothing, (cores, load, avail_gb)) && return 0
    return floor(
        Int,
        max(
            0.0,
            min(
                (cores * saturation - load - reserved_cores) / ncpus,
                (avail_gb * saturation - reserved_gb) / mem_gb
            )
        )
    )
end

function hpc_capacity(host, ncpus, mem_gb, saturation)
    text = try
        read(
            pipeline(
                `ssh -o BatchMode=yes -o ConnectTimeout=10 $host "nproc; cat /proc/loadavg; free -g | sed -n 2p"`,
                stderr = devnull
            ), String
        )
    catch
        ""
    end
    isempty(text) && @warn "HPC $host unreachable; assigning zero capacity"
    reserved_cores, reserved_gb = reserved_on(host)
    return parse_hpc_capacity(
        text, ncpus, mem_gb, saturation; reserved_cores,
        reserved_gb
    )
end

# Shave capacities by `buffer` for fill-everything requests; returns the fill target
function fill_target(cluster, hpc, buffer)
    scale(caps) = [k => floor(Int, (1 - buffer) * c) for (k, c) in caps]
    cluster, hpc = scale(cluster), scale(hpc)
    return cluster, hpc, sum(last, cluster; init = 0) + sum(last, hpc; init = 0)
end

# Largest-remainder split of n workers across capacities; assumes n <= total capacity
function proportional_split(n, caps)
    total = sum(last, caps; init = 0)
    (n <= 0 || total == 0) && return [first(c) => 0 for c in caps]
    quotas = [n * last(c) / total for c in caps]
    alloc = [min(floor(Int, q), last(c)) for (q, c) in zip(quotas, caps)]
    while sum(alloc) < n
        headroom = [
            alloc[j] < last(caps[j]) ? quotas[j] - alloc[j] : -Inf
                for j in eachindex(caps)
        ]
        i = argmax(headroom)
        headroom[i] == -Inf && break
        alloc[i] += 1
    end
    return [first(c) => a for (c, a) in zip(caps, alloc)]
end

"""
    allocate_workers(np, cluster, hpc, hpcratio) -> (cluster_alloc, hpc_alloc, shortfall)

Split `np` workers between cluster queues and HPCs, each a Vector{Pair{String, Int}} of
capacities. Pools are weighted by capacity x ratio: `hpcratio = 0` uses only the
cluster, `1` only the HPCs, `0.5` follows free capacity; intermediate values bias the
split, with overflow spilling to the other pool.
"""
function allocate_workers(np, cluster, hpc, hpcratio)
    0 <= hpcratio <= 1 ||
        throw(ArgumentError("hpcratio must be in [0, 1], got $hpcratio"))
    C = sum(last, cluster; init = 0)
    H = sum(last, hpc; init = 0)
    wc = C * (1 - hpcratio)
    wh = H * hpcratio
    n_h = if hpcratio == 1
        min(np, H)
    elseif hpcratio == 0 || wc + wh == 0
        0
    else
        min(round(Int, np * wh / (wc + wh)), H)
    end
    n_c = hpcratio == 1 ? 0 : min(np - n_h, C)
    if 0 < hpcratio < 1
        n_h = min(np - n_c, H) # spill what the cluster couldn't take
    end
    return proportional_split(n_c, cluster), proportional_split(n_h, hpc),
        np - n_c - n_h
end

"""
    distributeprocs(np; kwargs...) -> Vector{Int}

Launch `np` workers across the PBS queues and shared HPCs, split according to live
free capacity. Cluster workers are submitted with enforced `ncpus`/`mem`/`walltime`;
HPC workers run over ssh with `-t ncpus` threads and a heap-size hint (1 requested
core = 1 CPU thread), throttled by `saturation`. If combined capacity falls short,
launches what fits and warns.

# Arguments
- `np`: Total number of workers; `Inf` fills all available capacity, less `buffer`
- `buffer::Real=0.1`: With `np = Inf`, fraction of each pool's capacity left free
- `ncpus::Integer=1`: Cores per worker
- `mem::Union{Real,String}=4`: Memory per worker (GB or string with units)
- `walltime::Union{Integer,String}=24`: Walltime for cluster jobs (hours or "HH:MM:SS")
- `hpcratio::Real=0.5`: Bias toward HPCs; 0 = all cluster, 1 = all HPC, 0.5 = follow capacity
- `saturation::Real=0.75`: Max fraction of an HPC's cores/available memory to occupy
- `queues=["defaultQ", "taiji"]`: PBS queues to draw on
- `hpcs=["orr", "cartman", "karl"]`: ssh-reachable shared machines
- `project=dirname(Base.active_project())`: Project directory for all workers
- `kwargs...`: Forwarded to `Distributed.addprocs`

# Examples
```julia
procs = distributeprocs(20; ncpus = 2, mem = 8, walltime = 12)
procs = distributeprocs(10; hpcratio = 0.9)  # mostly onto the HPCs
procs = distributeprocs(Inf; ncpus = 2)      # fill 90% of everything
```
"""
function distributeprocs(
        np::Real;
        buffer::Real = 0.1,
        ncpus::Integer = 1,
        mem::Union{Real, AbstractString} = 4,
        walltime::Union{Integer, AbstractString} = 24,
        hpcratio::Real = 0.5,
        saturation::Real = 0.75,
        queues = ["defaultQ", "taiji"],
        hpcs = ["orr", "cartman", "karl"],
        project = dirname(Base.active_project()),
        kwargs...
    )
    (np > 0 && (isinf(np) || isinteger(np))) ||
        throw(ArgumentError("np must be a positive integer or Inf, got $np"))
    0 <= buffer < 1 || throw(ArgumentError("buffer must be in [0, 1), got $buffer"))
    mem_str = parse_memory(mem)
    mem_gb = memory_string_to_gb(mem_str)

    # Probe concurrently; each probe is one blocking ssh round-trip
    cluster_task = @async cluster_capacities(queues, ncpus, mem_gb)
    hpc = asyncmap(h -> h => hpc_capacity(h, ncpus, mem_gb, saturation), hpcs)
    cluster = fetch(cluster_task)

    if isinf(np)
        cluster, hpc, np = fill_target(cluster, hpc, buffer)
        np > 0 || (@warn "No capacity available to fill"; return Int[])
    end
    np = Int(np)
    calloc, halloc, shortfall = allocate_workers(np, cluster, hpc, hpcratio)
    @info "distributeprocs allocation" cluster = calloc hpc = halloc shortfall
    shortfall > 0 &&
        @warn "Capacity for only $(np - shortfall) of $np workers; launching those"

    procs = Int[]
    for (q, n) in calloc
        n > 0 || continue
        try
            append!(
                procs,
                addprocs(
                    n; ncpus, mem, walltime, queue = Cmd([String(q)]),
                    project = Cmd([String(project)]), kwargs...
                )
            )
        catch e
            @warn "Failed to launch $n workers on queue $q" exception = e
        end
    end

    exename = joinpath(Sys.BINDIR, "julia") # shared filesystem: same binary everywhere
    exeflags = with_heap_hint(`--project=$(project) -t $(ncpus)`, mem_str)
    for (h, n) in halloc
        n > 0 || continue
        try
            new = Distributed.addprocs(
                [(h, n)]; tunnel = true, exename, exeflags,
                dir = pwd(), enable_threaded_blas = true,
                kwargs...
            )
            append!(procs, new)
            ours = get!(
                Vector{@NamedTuple{pid::Int, cpus::Int, gb::Float64}},
                HPC_WORKERS, h
            )
            append!(ours, [(pid = p, cpus = Int(ncpus), gb = mem_gb) for p in new])
        catch e
            @warn "Failed to launch $n workers on $h" exception = e
        end
    end
    return procs
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

    pbsid = split(ENV["PBS_JOBID"], ".") |> first
    @info "Nuking job $pbsid"
    run(`ssh headnode "/opt/pbs/bin/qdel $pbsid"`)
    return @info "Nuked job $pbsid."  # This likely won't run if successful
end

end # module USydPhysics
