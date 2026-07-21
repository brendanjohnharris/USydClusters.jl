using AcademicClusters
import AcademicClusters: NCIGadi
import AcademicClusters.NCIGadi: gadi_defaults
import Preferences
using Distributed
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
        (; ncpus = 12, mem = 47, jobfs = 10, ngpus = 0, walltime = 12)
    @test gadi_defaults("gpuvolta") ==
        (; ncpus = 12, mem = 95, jobfs = 10, ngpus = 1, walltime = 12)
    @test gadi_defaults("dgxa100") ==
        (; ncpus = 16, mem = 250, jobfs = 10, ngpus = 1, walltime = 12)
    @test gadi_defaults("copyq") ==
        (; ncpus = 1, mem = 16, jobfs = 10, ngpus = 0, walltime = 2)
    @test gadi_defaults("gpuhopper") ==
        (; ncpus = 12, mem = 256, jobfs = 10, ngpus = 1, walltime = 12)
    @test gadi_defaults("megamembw") ==
        (; ncpus = 32, mem = 1500, jobfs = 10, ngpus = 0, walltime = 12) # min request is half a node
    @test gadi_defaults("hugemem").mem == 367
    @test gadi_defaults("express").walltime == 6 # a quarter of the 24h cap
    @test gadi_defaults("Express").walltime == 6 # case-insensitive
    unknown = @test_logs (:warn, r"Unknown Gadi queue") gadi_defaults("nonexistent")
    @test unknown == gadi_defaults("normal")
end

@testset "SU-neutral completion" begin
    # ncpus given: memory fills to that share, charge stays at ncpus
    @test gadi_defaults("normal"; ncpus = 4).mem == 15 # floor(4 * 190/48)
    @test gadi_defaults("normal"; ncpus = 4).ncpus == 4
    # mem given: cores fill to what the memory share pays for
    @test gadi_defaults("hugemem"; mem = 300).ncpus == 9 # floor(300/1470 * 48)
    @test gadi_defaults("hugemem"; mem = 300).mem == 300
    @test gadi_defaults("normal"; mem = "100GB").ncpus == 25
    @test gadi_defaults("normal"; mem = "100GB").mem == "100GB" # units preserved
    # beyond one node's memory: whole nodes
    @test gadi_defaults("normal"; mem = 300).ncpus == 96
    # queue minimum still enforced on derived cores
    @test gadi_defaults("megamembw"; mem = 500).ncpus == 32
    # GPU queues: ngpus drives both; either other resource fills the GPU count
    @test gadi_defaults("gpuvolta"; ngpus = 2) ==
        (; ncpus = 24, mem = 191, jobfs = 10, ngpus = 2, walltime = 12)
    @test gadi_defaults("gpuvolta"; ncpus = 24).ngpus == 2
    @test gadi_defaults("gpuvolta"; mem = 300).ngpus == 3 # floor(300/382 * 4)
    @test gadi_defaults("gpuvolta"; mem = 300).ncpus == 36
    # explicit fields are never altered
    @test gadi_defaults("normal"; ncpus = 4, mem = 100) ==
        (; ncpus = 4, mem = 100, jobfs = 10, ngpus = 0, walltime = 12)
end

@testset "walltime fraction" begin
    # A set preference outranks the environment, so only assert env behaviour when unset
    if isnothing(Preferences.load_preference(AcademicClusters, "gadi_maxwalltime_fraction"))
        withenv("ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION" => nothing) do
            @test gadi_defaults("normal").walltime == 12 # default 1/4 of the 48h cap
        end
        withenv("ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION" => "1") do
            @test gadi_defaults("normal").walltime == 48
        end
        withenv("ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION" => "1/2") do
            @test gadi_defaults("normal").walltime == 24
            @test gadi_defaults("copyq").walltime == 5
        end
        withenv("ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION" => "0.125") do
            @test gadi_defaults("normal").walltime == 6
        end
        withenv("ACADEMICCLUSTERS_GADI_MAXWALLTIME_FRACTION" => "2") do
            @test_throws ArgumentError gadi_defaults("normal")
        end
    end
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

@testset "distributeprocs splitting" begin
    lines = ["gadi-cpu-clx-2234", "gadi-cpu-clx-2234", "gadi-cpu-clx-2235",
        "gadi-cpu-clx-2235", ""]
    @test NCIGadi.nodefile_hosts(lines) == ["gadi-cpu-clx-2234", "gadi-cpu-clx-2235"]
    @test NCIGadi.even_split(4, ["a", "b"]) == ["a" => 2, "b" => 2]
    @test NCIGadi.even_split(10, ["a", "b", "c"]) == ["a" => 4, "b" => 3, "c" => 3]
    @test NCIGadi.even_split(1, ["a", "b"]) == ["a" => 1, "b" => 0]
    # PBS_NCPUS outranks the line count (robust to mpiprocs != ncpus selects)
    withenv("PBS_NCPUS" => "96") do
        @test NCIGadi.job_slots(["a", "b"]) == 96
    end
    withenv("PBS_NCPUS" => nothing) do
        @test NCIGadi.job_slots(["a", "a", "b", ""]) == 3 # per-rank fallback
    end
    withenv("PBS_NODEFILE" => nothing) do
        @test_throws ErrorException NCIGadi.distributeprocs(2)
    end
end

@testset "distributeprocs local spawn" begin
    # Single-host nodefile naming this machine exercises the LocalManager path
    nodefile = tempname()
    write(nodefile, join(fill(first(split(gethostname(), '.')), 2), '\n'))
    withenv("PBS_NODEFILE" => nodefile) do
        procs = @test_logs (:info, r"allocation") match_mode = :any NCIGadi.distributeprocs(2)
        @test length(procs) == 2
        @test remotecall_fetch(() -> ENV["OPENBLAS_NUM_THREADS"], first(procs)) == "1"
        @test remotecall_fetch(Threads.nthreads, first(procs)) == 1
        rmprocs(procs)
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
