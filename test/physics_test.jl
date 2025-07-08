using USydClusters
import USydClusters: Physics
using Test
using UUIDs

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
