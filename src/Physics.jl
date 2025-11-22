module Physics
using Distributed
import USydClusters: build_julia_command, format_pbs_resources, LOGDIR, to_string

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
parse_memory(0.5)       # "500MB"
parse_memory(16)        # "16GB"
parse_memory(1.5)       # "1536MB"
parse_memory("123KB")   # "123KB"
parse_memory("16GB")    # "16GB"
```
"""
function parse_memory(mem::Real)
    mem > 0 || throw(ArgumentError("Memory must be positive, got $mem GB"))

    # Convert GB input to optimal unit
    if mem >= 1 && isinteger(mem)
        # For whole GB values, keep as GB
        return "$(Int(mem))GB"
    elseif mem < 1
        # For values less than 1GB, convert to MB
        mb = mem * 1024
        if isinteger(mb)
            return "$(Int(mb))MB"
        else
            return "$(Int(round(mb)))MB"
        end
    else
        # For fractional GB values > 1, convert to MB for precision
        mb = mem * 1024
        return "$(Int(round(mb)))MB"
    end
end

function parse_memory(mem::AbstractString)
    # For strings, validate and return as-is
    mem = strip(mem)

    # Validate format
    if !occursin(r"^\d+(?:\.\d+)?\s*[KMGT]B$"i, mem)
        throw(ArgumentError("Invalid memory format: '$mem'. Use format like '16GB', '2048MB', '512KB', or '2TB'"))
    end

    # Return the string as provided (preserving user's choice of units)
    return mem
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

    # Convert hours to HH:MM:SS format
    # Note: PBS allows hours > 99, so we don't limit to 2 digits
    hours_str = lpad(walltime, 2, '0')
    return "$(hours_str):00:00"
end

function parse_walltime(walltime::AbstractString)
    walltime = strip(walltime)

    # Validate HH:MM:SS format
    if !occursin(r"^\d+:\d{2}:\d{2}$", walltime)
        throw(ArgumentError("Invalid walltime format: '$walltime'. Expected format: HH:MM:SS (e.g., '24:00:00', '168:00:00')"))
    end

    # Parse and validate components
    parts = split(walltime, ':')
    hours = parse(Int, parts[1])
    minutes = parse(Int, parts[2])
    seconds = parse(Int, parts[3])

    # Validate ranges
    hours >= 0 || throw(ArgumentError("Hours must be non-negative"))
    0 <= minutes <= 59 || throw(ArgumentError("Minutes must be between 0 and 59"))
    0 <= seconds <= 59 || throw(ArgumentError("Seconds must be between 0 and 59"))

    # Total time must be positive
    total_seconds = hours * 3600 + minutes * 60 + seconds
    total_seconds > 0 || throw(ArgumentError("Total walltime must be greater than 0"))

    # Return the validated string (maintaining original format)
    return walltime
end

"""
    memory_string_to_gb(mem_str::String) -> Float64

Extract numeric GB value from memory string for calculations.

# Arguments
- `mem_str`: Memory string with units (e.g., "16GB", "2048MB")

# Returns
- Memory value in GB as Float64

# Internal use only - for heap size calculations
"""
function memory_string_to_gb(mem_str::String)
    m = match(r"^(\d+(?:\.\d+)?)\s*([KMGT]B)$"i, mem_str)
    isnothing(m) && return 16.0  # Default fallback

    value = parse(Float64, m[1])
    unit = uppercase(m[2])

    if unit == "KB"
        return value / (1024 * 1024)
    elseif unit == "MB"
        return value / 1024
    elseif unit == "GB"
        return value
    elseif unit == "TB"
        return value * 1024
    else
        return 16.0  # Default fallback
    end
end

"""
    worker_cookie() -> String

Generate a cookie for Distributed worker authentication.
"""
function worker_cookie()
    Distributed.init_multi()
    Distributed.cluster_cookie()
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
- `mem::String`: Memory per worker with units (e.g., "16GB", "2048MB")
- `walltime::String`: Maximum walltime in HH:MM:SS format
- `queue::Cmd`: PBS queue name (optional)
- `project::Cmd`: Project directory (optional)
- `qsubflags::Cmd`: Additional qsub flags (optional)
"""
struct PBSProManager <: ClusterManager
    np::Integer
    ncpus::Integer
    mem::String  # Memory with units
    walltime::String  # HH:MM:SS format
    queue::Cmd
    project::Cmd
    qsubflags::Cmd
end

"""
    PBSProManager(np=1; ncpus=8, mem=16, walltime=24, queue=``, project=``, qsubflags=``)

Create a PBS Pro cluster manager.

# Arguments
- `np::Integer=1`: Number of parallel workers to launch
- `ncpus::Integer=8`: Number of CPUs per worker
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
```
"""
function PBSProManager(np::Integer = 1;
                       ncpus::Integer = 8,
                       mem::Union{Real, AbstractString} = 16,
                       walltime::Union{Integer, AbstractString} = 24,
                       queue::Cmd = ``,
                       project::Cmd = ``,
                       qsubflags::Cmd = ``)
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    np > 0 || throw(ArgumentError("np must be a positive integer, got $np"))
    ncpus > 0 || throw(ArgumentError("ncpus must be a positive integer, got $ncpus"))

    return PBSProManager(np, ncpus, mem_str, walltime_str, queue, project, qsubflags)
end

"""
    Distributed.manage(manager::PBSProManager, id::Int64, config::WorkerConfig, op::Symbol)

Manage worker lifecycle operations. Currently a no-op for PBS Pro.
"""
function Distributed.manage(manager::PBSProManager,
                            id::Int64, config::WorkerConfig, op::Symbol)
    # No-op for PBS
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
function Distributed.launch(manager::PBSProManager,
                            params::Dict, instances_arr::Array, c::Condition)
    try
        dir = params[:dir]
        exename = params[:exename]
        exeflags = params[:exeflags]

        np = manager.np
        ncpus = manager.ncpus
        mem = manager.mem  # Now a string with units
        walltime = manager.walltime  # Now in HH:MM:SS format

        # Format queue option
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

        # Setup job array and logging
        if np == 1
            Jcmd = ""
            logdir = `$(LOGDIR)`
            logfile = `$(to_string(logdir))/\$\{MAIN_JOBID\}.$(ID).log`
        elseif np > 1
            Jcmd = "#PBS -J 1-$np"
            logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
            logfile = `$(to_string(logdir))/\$\{PBS_ARRAY_INDEX\}.log`
        else
            throw(ArgumentError("np must be a positive integer, got $np"))
        end

        # Build Julia command with heap size hint
        script = worker_arg()
        # Calculate heap size hint from memory string
        mem_gb = memory_string_to_gb(mem)
        heap_size_gb = ceil(Int, mem_gb / 2)
        exeflags = `$exeflags --heap-size-hint=$(heap_size_gb)G`
        julia_cmd = build_julia_command(; exename, exeflags, project, script, logfile)

        # Create PBS script - format_pbs_resources now receives strings directly
        ID = Base.shell_escape("$(ID)")
        cmd = """#!/bin/bash
        #PBS -N julia-$ID
        #PBS -V
        #PBS -j oe
        #PBS -m n
        #PBS -o $(LOGDIR)/$ID.final.log
        $(Jcmd)
        #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem)),walltime=$(walltime)
        cd $dir
        source $(ENV["HOME"])/.bashrc
        MAIN_JOBID=\${PBS_JOBID%\\[*}
        MAIN_JOBID=\${MAIN_JOBID%.*}
        mkdir -p "$(to_string(logdir))"
        $(to_string(julia_cmd))
        """
        @debug cmd

        # Write and submit script
        f = tempname(LOGDIR)
        write(f, cmd)

        _qsub = `/opt/pbs/bin/qsub $(queue) $(qsubflags)`
        qsub = "source $(ENV["HOME"])/.bashrc > /dev/null 2>&1 && $(Base.shell_escape(_qsub)) $(Base.shell_escape(f))"
        qsub_cmd = pipeline(`ssh headnode "$qsub"`, stderr = devnull)

        @debug "Submitting PBS job: $qsub_cmd"

        # Capture both stdout and potential errors
        output = ""
        try
            output = read(qsub_cmd, String)
        catch e
            rm(f, force = true)
            error("Failed to submit PBS job: $e")
        end

        # Clean up submission script
        rm(f, force = true)

        # Parse job ID from output
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
        tunnels_to_cleanup = []

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
                    config.userdata = Dict{Symbol, Any}(:job => id, :task => i,
                                                        :iofile => fname)
                else
                    # Setup SSH tunnel with retry logic for port conflicts
                    tunnel_established = false
                    tunnel_proc = nothing
                    max_port_attempts = 10

                    for attempt in 1:max_port_attempts
                        local_port = rand(10000:60000)

                        # Add timeout and better error handling to SSH command
                        tunnel_cmd = `ssh -4 -N -L 127.0.0.1:$local_port:$host:$port -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 headnode`

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
                                config.userdata = Dict{Symbol, Any}(:job => id,
                                                                    :task => i,
                                                                    :iofile => fname,
                                                                    :tunnel => tunnel_proc)
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

        rm(f, force = true)
        logloc = np == 1 ? logfile : logdir
        println("Running. See stdout of children at $logloc")
        return id

    catch e
        println("Error launching workers: $e")
        @error "Full error details" exception=(e, catch_backtrace())

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
    if haskey(config.userdata, :job)
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
```
"""
function addprocs(np::Integer;
                  ncpus::Integer = 8,
                  mem::Union{Real, AbstractString} = 16,
                  walltime::Union{Integer, AbstractString} = 24,
                  queue::Cmd = ``,
                  project::Cmd = ``,
                  qsubflags::Cmd = ``,
                  kwargs...)
    Distributed.addprocs(PBSProManager(np; ncpus, mem, walltime, queue, project, qsubflags);
                         enable_threaded_blas = true,
                         kwargs...)
end

# ===========================
# Script Execution Functions
# ===========================

"""
    capture_jobid(cmd::Cmd) -> String

Capture the job ID from a qsub command output.

# Arguments
- `cmd`: Command to execute

# Returns
- Job ID string
"""
function capture_jobid(cmd::Cmd)
    output = read(cmd, String)
    jobid, hostname = split(output, '.')
    return jobid
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
function runscript(script::String;
                   ncpus::Integer = 10,
                   mem::Union{Real, AbstractString} = 31,
                   walltime::Union{Integer, AbstractString} = 48,
                   qsubflags::Cmd = ``,
                   project::Cmd = ``,
                   exeflags::Cmd = ``,
                   queue::Cmd = ``,
                   kwargs...)
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    # Calculate heap size hint
    mem_gb = memory_string_to_gb(mem_str)
    heap_size_gb = ceil(Int, mem_gb / 2)

    ID = script |> Base.splitext |> first |> Base.splitpath |> last |> Base.shell_escape
    logfile = `$(LOGDIR)/\$\{PBS_JOBID\}.$(ID).log`
    exeflags = `$exeflags --heap-size-hint=$(heap_size_gb)G`

    julia_cmd = build_julia_command(; exeflags, project, script, logfile, kwargs...)

    cmd = """#!/bin/bash
    #PBS -N julia-$(ID)
    #PBS -V
    #PBS -j oe
    #PBS -m n
    #PBS -o $(LOGDIR)/$ID.final.log
    #PBS -l ncpus=$(ncpus),mem=$(lowercase(mem_str)),walltime=$(walltime_str)
    source $(ENV["HOME"])/.bashrc
    cd $project
    $(to_string(julia_cmd))
    """

    qsub_file = first(mktemp(LOGDIR; cleanup = false))
    open(qsub_file, "w") do f
        write(f, cmd)
    end

    queue = isempty(queue) ? queue : "-q $(Base.shell_escape(queue))"
    qsub = "source $(ENV["HOME"])/.bashrc && /opt/pbs/bin/qsub $(to_string(qsubflags)) $queue $(Base.shell_escape(qsub_file))"
    qsub_cmd = `ssh headnode "$qsub"`
    jobid = capture_jobid(qsub_cmd)

    return jobid, replace(to_string(logfile), r"\$\{PBS_JOBID\}" => "$jobid.headnode")
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
    open(file, "w") do f
        write(f, string(expr))
    end
    runscript(file; kwargs...)
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
function runscripts(scriptdir::String;
                    ncpus::Integer = 10,
                    mem::Union{Real, AbstractString} = 31,
                    walltime::Union{Integer, AbstractString} = 48,
                    qsubflags::Cmd = ``,
                    project::Cmd = ``,
                    exeflags::Cmd = ``,
                    queue::Cmd = ``,
                    ID = rand(UInt16) |> Int,
                    kwargs...)
    mem_str = parse_memory(mem)
    walltime_str = parse_walltime(walltime)

    # Calculate heap size hint
    mem_gb = memory_string_to_gb(mem_str)
    heap_size_gb = ceil(Int, mem_gb / 2)

    script = `$(scriptdir)/\$\{PBS_ARRAY_INDEX\}.jl`
    logdir = `$(LOGDIR)/\$\{MAIN_JOBID\}\[\].$(ID).log`
    logfile = `$(to_string(logdir))/\$\{PBS_ARRAY_INDEX\}.log`
    exeflags = `$exeflags --heap-size-hint=$(heap_size_gb)G`
    N = length(readdir(scriptdir))

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
    qsub = "source $(ENV["HOME"])/.bashrc && /opt/pbs/bin/qsub $(to_string(qsubflags)) $queue $qsub_file"
    qsub_cmd = `ssh headnode "$qsub"`

    @info "Submitting array job with name julia-$ID (logdir: $LOGDIR)"
    jobid = capture_jobid(qsub_cmd)

    return jobid
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
    @info "Nuked job $pbsid."  # This likely won't run if successful
end

end # module Physics
