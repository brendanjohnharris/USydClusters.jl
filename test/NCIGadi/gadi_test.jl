using AcademicClusters
import AcademicClusters: NCIGadi
using Test
using UUIDs

# Integration tests: submit real jobs, so only meaningful on a Gadi login node
# with the gadi_project (and usually gadi_storage) preferences set.

ENV["JULIA_DEBUG"] = "AcademicClusters"

# Queue wait on Gadi can be minutes even for tiny jobs
const TIMEOUT = 600

function waitfor(files...; timeout = TIMEOUT)
    start_time = time()
    while time() - start_time < timeout
        all(isfile, files) && return true
        sleep(2)
    end
    return false
end

@testset "runscript" begin
    tempfile = abspath("gadi_test.txt")
    key = UUIDs.uuid4() |> string
    pair = "$(tempfile): $key"
    expr = quote
        open($tempfile, "w") do io
            write(io, $key)
        end
        print($pair)
    end

    rm(tempfile, force = true)
    jobid, logfile = NCIGadi.runscript(expr; ncpus = 1, mem = 1, walltime = 1)
    @test !isnothing(tryparse(Int, jobid))

    @test waitfor(tempfile, logfile)
    sleep(1) # Ensure the file is written before reading
    @test read(tempfile, String) == key
    @test read(logfile, String) == pair
    rm(tempfile, force = true)
end

@testset "runscripts" begin
    uuids = [UUIDs.uuid4() |> string for _ in 1:2]
    files = [abspath("gadi_test_$i.txt") for i in 1:length(uuids)]
    rm.(files, force = true)
    exprs = map(uuids, files) do key, tempfile
        quote
            open($tempfile, "w") do io
                write(io, $key)
            end
        end
    end

    jobs = NCIGadi.runscripts(exprs; ncpus = 1, mem = 1, walltime = 1)
    @test length(jobs) == length(exprs)

    @test waitfor(files...)
    sleep(1)
    foreach(uuids, files) do key, tempfile
        @test read(tempfile, String) == key
    end
    rm.(files, force = true)
end
