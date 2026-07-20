using AcademicClusters
import AcademicClusters: NCIGadi
import AcademicClusters.NCIGadi: gadi_defaults
import Preferences
using Test

# Off-cluster unit tests: script generation and preference plumbing only.

@testset "pbs_script" begin
    julia_cmd = AcademicClusters.build_julia_command(;
        project = `/g/data/ab12/proj`, script = "test.jl", logfile = "log.log"
    )
    kw = (;
        ID = "test", julia_cmd, ncpus = 12, mem = "47GB", walltime = "48:00:00",
        jobfs = "10GB", ngpus = 0, queue = "normal", project_code = "ab12",
        project = `/g/data/ab12/proj`,
    )

    script = NCIGadi.pbs_script(; kw..., storage = "gdata/ab12+scratch/ab12")
    @test occursin("#PBS -N julia-test", script)
    @test occursin("#PBS -P ab12", script)
    @test occursin("#PBS -q normal", script)
    @test occursin("#PBS -l ncpus=12,mem=47gb,walltime=48:00:00,jobfs=10gb", script)
    @test occursin("#PBS -l storage=gdata/ab12+scratch/ab12", script)
    @test !occursin("#PBS -V", script) # discouraged on Gadi
    @test !occursin("ngpus", script)
    @test occursin("cd /g/data/ab12/proj", script)
    @test occursin("test.jl", script)

    script = NCIGadi.pbs_script(; kw..., storage = "")
    @test !occursin("storage=", script)

    script = NCIGadi.pbs_script(; kw..., ngpus = 1, queue = "gpuvolta", storage = "")
    @test occursin("jobfs=10gb,ngpus=1", script)
end

@testset "gadi_defaults" begin
    @test gadi_defaults("normal") ==
        (; ncpus = 12, mem = 47, jobfs = 10, ngpus = 0, walltime = 48)
    @test gadi_defaults("gpuvolta") ==
        (; ncpus = 12, mem = 95, jobfs = 10, ngpus = 1, walltime = 48)
    @test gadi_defaults("dgxa100") ==
        (; ncpus = 16, mem = 250, jobfs = 10, ngpus = 1, walltime = 48)
    @test gadi_defaults("copyq") ==
        (; ncpus = 1, mem = 16, jobfs = 10, ngpus = 0, walltime = 10)
    @test gadi_defaults("hugemem").mem == 367
    @test gadi_defaults("express").walltime == 24
    @test gadi_defaults("Express").walltime == 24 # case-insensitive
    unknown = @test_logs (:warn, r"Unknown Gadi queue") gadi_defaults("nonexistent")
    @test unknown == gadi_defaults("normal")
end

@testset "default_project" begin
    d = AcademicClusters.default_project()
    @test d isa Cmd
    @test only(d.exec) == dirname(Base.active_project())
end

@testset "combine_exprs" begin
    setup = quote
        using Statistics
    end
    body = quote
        x = 1
        print(x)
    end
    file = tempname()
    AcademicClusters.write_exprs(file, AcademicClusters.combine_exprs(setup, body))
    @test readlines(file) == ["using Statistics", "x = 1", "print(x)"]

    AcademicClusters.write_exprs(file, AcademicClusters.combine_exprs(setup, :(print(2)))) # non-block body
    @test readlines(file) == ["using Statistics", "print(2)"]

    @test AcademicClusters.combine_exprs(nothing, body) === body
end

@testset "runscript kwarg chain" begin
    # Exercises defaults resolution, parsing, and script generation end to end;
    # only safe where qsub is absent, so submission itself is the failure point
    if isnothing(Sys.which("qsub"))
        withenv("ACADEMICCLUSTERS_GADI_PROJECT" => "ab12") do
            @test_throws Base.IOError NCIGadi.runscript("dummy.jl"; queue = "gpuvolta")
        end
    end
end

@testset "capture_jobid" begin
    @test AcademicClusters.capture_jobid(`echo 12345678.gadi-pbs`) ==
        ("12345678", "gadi-pbs")
    @test_throws ErrorException AcademicClusters.capture_jobid(`echo notajob`)
end

@testset "gadi preferences" begin
    # A set preference outranks the environment, so only assert env behaviour when unset
    if isnothing(Preferences.load_preference(AcademicClusters, "gadi_project"))
        withenv("ACADEMICCLUSTERS_GADI_PROJECT" => "xy99") do
            @test NCIGadi.gadi_project() == "xy99"
        end
        withenv("ACADEMICCLUSTERS_GADI_PROJECT" => nothing) do
            @test_throws ErrorException NCIGadi.gadi_project()
        end
    end
    if isnothing(Preferences.load_preference(AcademicClusters, "gadi_queue"))
        withenv("ACADEMICCLUSTERS_GADI_QUEUE" => nothing) do
            @test AcademicClusters.pref_or_env("gadi_queue", "normal") == "normal"
        end
        withenv("ACADEMICCLUSTERS_GADI_QUEUE" => "express") do
            @test AcademicClusters.pref_or_env("gadi_queue", "normal") == "express"
        end
    end
end
