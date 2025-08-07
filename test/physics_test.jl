using USydClusters
import USydClusters: Physics
using Test
using Distributed
using UUIDs

ENV["JULIA_DEBUG"] = "USydClusters"

@testset "runscript" begin
    script = abspath("testscript.jl")
    tempfile = abspath("test.txt")
    key = UUIDs.uuid4() |> string

    # * From file
    rm(tempfile, force = true)
    jobid, logfile = Physics.runscript(script; ncpus = 1, mem = 1, walltime = 1,
                                       args = [tempfile, key])

    start_time = time()
    timeout = 30
    while time() - start_time < timeout
        if isfile(tempfile) && isfile(logfile)
            break
        end
    end
    if !isfile(tempfile)
        error("Output file $tempfile not found after $timeout seconds")
    end
    if !isfile(logfile)
        error("Logfile $logfile not found after $timeout seconds")
    end

    sleep(1) # Ensure the file is written before reading
    @test isfile(tempfile)
    @test read(tempfile, String) == key
    @test read(logfile, String) == "$tempfile: $(key)"
    rm(tempfile, force = true)

    # * From Expr
    key = UUIDs.uuid4() |> string
    pair = "$(abspath(tempfile)): $key"
    expr = quote
        mkpath(first(Base.splitdir($tempfile)))
        open($tempfile, "w") do io
            write(io, $key)
        end
        print($pair)
    end
    jobid, logfile = Physics.runscript(expr; ncpus = 1, mem = 1, walltime = 1)

    start_time = time()
    timeout = 30
    while time() - start_time < timeout
        if isfile(tempfile) && isfile(logfile)
            break
        end
    end
    if !isfile(tempfile)
        error("Output file $tempfile not found after $timeout seconds")
    end
    if !isfile(logfile)
        error("Logfile $logfile not found after $timeout seconds")
    end

    sleep(1) # Ensure the file is written before reading
    @test isfile(tempfile)
    @test read(tempfile, String) == key
    @test read(logfile, String) == "$tempfile: $(key)"
end

@testset "runscripts" begin
    uuids = [UUIDs.uuid4() |> string for _ in 1:2]
    files = [abspath("test/test_$i.txt") for i in 1:length(uuids)]
    rm.(files, force = true)
    exprs = map(uuids, files) do key, tempfile
        pair = "$(abspath(tempfile)): $key"
        quote
            mkpath(first(Base.splitdir($tempfile)))
            open($tempfile, "w") do io
                write(io, $key)
            end
            print($pair)
        end
    end
    jobid = Physics.runscripts(exprs; ncpus = 1, mem = 1, walltime = 1, queue = "taiji")

    map(uuids, files) do key, tempfile
        start_time = time()
        timeout = 30
        while time() - start_time < timeout
            if isfile(tempfile)
                break
            end
        end
        if !isfile(tempfile)
            error("Output file $tempfile not found after $timeout seconds")
        end

        sleep(1) # Ensure the file is written before reading
        @test isfile(tempfile)
        @test read(tempfile, String) == key
    end
end

@testset "addproc" begin
    try
        ourprocs = USydClusters.Physics.addprocs(1; mem = 4, ncpus = 1, walltime = 1,
                                                 queue = "h100")
        @test ourprocs == [2]
        @test nprocs() == 2      # Total processes should be main (1) + new (1)
        @test workers() == [2]   # The list of worker IDs should match

        @test remotecall_fetch(myid, ourprocs[1]) == 2

        @everywhere begin
            f() = 13^3
        end
        @test remotecall_fetch(f, ourprocs[1]) == 2197

        jobid = fetch(@spawnat only(ourprocs) ENV["PBS_JOBID"])

        rmprocs(ourprocs)
        stillthere = true
        start_time = time()
        while stillthere && time() - start_time < 30
            output = read(`qstat`, String)
            stillthere = occursin(string(jobid), output)
            sleep(1)
        end
        @test !stillthere

        rmprocs(ourprocs)
        @test nprocs() == 1
    finally
        if nprocs() > 1
            rmprocs()
        end
    end
end
@testset "addprocs" begin
    try
        np = 2
        ourprocs = USydClusters.Physics.addprocs(np; mem = 4, ncpus = 1, walltime = 1,
                                                 queue = `h100`)
        @test nprocs() == np + 1      # Total processes should be main (1) + new (10)
        @test workers() == ourprocs   # The list of worker IDs should match

        @everywhere begin
            f() = 13^3
        end
        map(ourprocs) do proc
            @test remotecall_fetch(myid, proc) == proc
            @test remotecall_fetch(f, proc) == 2197
        end

        # Parse the output of qstat and check the job is gone
        jobids = map(ourprocs) do proc
            id = @spawnat proc ENV["PBS_JOBID"]
            return fetch(id)
        end

        rmprocs(ourprocs)
        stillthere = true
        start_time = time()
        while stillthere && time() - start_time < 30
            output = read(`qstat -t`, String)
            stillaround = map(jobids) do jobid
                occursin(string(jobid), output)
            end
            stillthere = any(stillaround)
            sleep(1)
        end
        @test !stillthere
        @test nprocs() == 1
    finally
        if nprocs() > 1
            rmprocs()
        end
    end
end
