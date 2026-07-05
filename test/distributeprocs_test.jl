import USydClusters: USydPhysics
using Test

# Fixtures modeled on real headnode output
const PBSNODES = """
nodegpu02
     state = free
     resources_available.mem = 0b
     resources_available.ncpus = 0
     resources_assigned.mem = 0kb
     resources_assigned.ncpus = 0
nodegpu02[0]
     state = free
     resources_available.mem = 62422mb
     resources_available.ncpus = 16
     resources_available.Qlist = physics,l40s
     resources_assigned.mem = 42896384kb
     resources_assigned.ncpus = 10
nodegpu02[1]
     state = free
     resources_available.mem = 63482mb
     resources_available.ncpus = 16
     resources_available.Qlist = physics,l40s
     resources_assigned.mem = 32601088kb
     resources_assigned.ncpus = 7
nodegpu02[3]
     state = offline
     resources_available.mem = 63477mb
     resources_available.ncpus = 16
     resources_available.Qlist = physics
     resources_assigned.mem = 0kb
     resources_assigned.ncpus = 0
taiji01[0]
     state = free
     resources_available.mem = 126gb
     resources_available.ncpus = 32
     resources_available.Qlist = taiji
     resources_assigned.mem = 0kb
     resources_assigned.ncpus = 0
"""

const QSTATQ = """
Queue: defaultQ
    queue_type = Route
    route_destinations = physics
    enabled = True

Queue: physics
    queue_type = Execution
    max_run_res.ncpus = [u:PBS_GENERIC=48]
    enabled = True

Queue: taiji
    queue_type = Execution
    acl_user_enable = True
"""

const QSTATU = """
headnode:
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
21290.headnode  bhar9988 physics  julia-123    1234   1   8   16gb 24:00 R 01:23
21291.headnode  bhar9988 physics  julia-124    1235   1   8   16gb 24:00 Q   --
"""

const PROBE = PBSNODES * "===Q===\n" * QSTATQ * "===U===\n" * QSTATU

@testset "string mem round-trip" begin
    # parse_memory returns a SubString; downstream conversion must accept it
    @test USydPhysics.memory_string_to_gb(USydPhysics.parse_memory("16GB")) == 16.0
    @test USydPhysics.memory_string_to_gb(USydPhysics.parse_memory(" 2048MB ")) == 2.0
end

@testset "pbs_size_gb" begin
    @test USydPhysics.pbs_size_gb("0b") == 0.0
    @test USydPhysics.pbs_size_gb("1024mb") == 1.0
    @test USydPhysics.pbs_size_gb("58720256kb") == 56.0
    @test USydPhysics.pbs_size_gb("126gb") == 126.0
    @test USydPhysics.pbs_size_gb("2tb") == 2048.0
    @test USydPhysics.pbs_size_gb("garbage") == 0.0
end

@testset "parse_pbsnodes" begin
    vnodes = USydPhysics.parse_pbsnodes(PBSNODES)
    @test length(vnodes) == 5
    v = only(filter(v -> v.name == "nodegpu02[0]", vnodes))
    @test v.freecpus == 6
    @test v.qlist == ["physics", "l40s"]
    @test isapprox(v.freegb, 62422 / 1024 - 42896384 / 1024^2; atol = 1e-6)
    @test only(filter(v -> v.name == "nodegpu02[3]", vnodes)).state == "offline"
end

@testset "parse_queues and user usage" begin
    q = USydPhysics.parse_queues(QSTATQ)
    @test q["defaultq"].type == "Route" && q["defaultq"].dest == "physics"
    @test q["physics"].ncpus_cap == 48
    @test isnothing(q["taiji"].ncpus_cap)
    usage = USydPhysics.parse_user_ncpus(QSTATU)
    @test usage == Dict("physics" => 8) # only the running job counts
end

@testset "cluster_capacities" begin
    caps = USydPhysics.cluster_capacities(PROBE, ["defaultQ", "taiji"], 2, 8.0)
    # physics vnodes (offline excluded): [0] has 6cpu/~21GB -> 2 fits; [1] has 9cpu/~31GB -> 3 fits
    # user cap: (48 - 8 running) / 2 cpus = 20 -> not binding
    # taiji01[0]: min(32 ÷ 2, 126 ÷ 8) = 15, memory-bound
    @test caps == ["defaultQ" => 5, "taiji" => 15]
    # tight user cap binds defaultQ: 1-cpu workers, 40 allowance left
    caps = USydPhysics.cluster_capacities(PROBE, ["defaultQ"], 1, 1.0)
    @test caps == ["defaultQ" => 15] # nodes fit 6+9=15 < 40 allowance
    @test USydPhysics.cluster_capacities("", ["defaultQ"], 1, 1.0) == ["defaultQ" => 0]
end

@testset "parse_hpc_capacity" begin
    out = "20\n6.10 5.84 6.31 3/2106 806800\nMem: 188 103 45 0 42 85\n"
    # cores: 20*0.75 - 6.1 = 8.9 workers; mem: 85*0.75/4 = 15.9 -> 8
    @test USydPhysics.parse_hpc_capacity(out, 1, 4.0, 0.75) == 8
    @test USydPhysics.parse_hpc_capacity(out, 4, 4.0, 0.75) == 2
    @test USydPhysics.parse_hpc_capacity("", 1, 4.0, 0.75) == 0
    @test USydPhysics.parse_hpc_capacity("8\n20.0 19.0 18.0 1/1 1\nMem: 62 42 0 0 19 19\n",
                                     1, 4.0, 0.75) == 0 # overloaded machine
    # existing workers reduce capacity: 4 reserved cores -> 8.9 - 4 = 4.9 workers
    @test USydPhysics.parse_hpc_capacity(out, 1, 4.0, 0.75; reserved_cores = 4) == 4
    # reserved memory binds: 85*0.75 - 48 = 15.75 -> 3 workers of 4GB
    @test USydPhysics.parse_hpc_capacity(out, 1, 4.0, 0.75; reserved_gb = 48.0) == 3
    @test USydPhysics.parse_hpc_capacity(out, 1, 4.0, 0.75; reserved_cores = 100) == 0
end

@testset "reserved_on" begin
    entry(pid) = (pid = pid, cpus = 2, gb = 4.0)
    empty!(USydPhysics.HPC_WORKERS)
    @test USydPhysics.reserved_on("orr") == (0, 0.0)
    # pid 1 (this process) is always live; pid 9999 is dead and gets pruned
    USydPhysics.HPC_WORKERS["orr"] = [entry(1), entry(9999)]
    @test USydPhysics.reserved_on("orr") == (2, 4.0)
    @test length(USydPhysics.HPC_WORKERS["orr"]) == 1 # dead entry pruned in place
    empty!(USydPhysics.HPC_WORKERS)
end

@testset "allocate_workers" begin
    cluster = ["defaultQ" => 10, "taiji" => 5]
    hpc = ["orr" => 3, "cartman" => 6, "karl" => 6]
    # extremes
    calloc, halloc, short = USydPhysics.allocate_workers(10, cluster, hpc, 0)
    @test sum(last, calloc) == 10 && sum(last, halloc) == 0 && short == 0
    calloc, halloc, short = USydPhysics.allocate_workers(20, cluster, hpc, 0)
    @test sum(last, calloc) == 15 && sum(last, halloc) == 0 && short == 5 # no spill at 0
    calloc, halloc, short = USydPhysics.allocate_workers(10, cluster, hpc, 1)
    @test sum(last, calloc) == 0 && sum(last, halloc) == 10 && short == 0
    # balanced: capacities 15 vs 15 -> even split
    calloc, halloc, short = USydPhysics.allocate_workers(10, cluster, hpc, 0.5)
    @test sum(last, calloc) == 5 && sum(last, halloc) == 5 && short == 0
    # per-target allocations never exceed capacity
    for (alloc, caps) in ((calloc, cluster), (halloc, hpc))
        @test all(last(a) <= last(c) for (a, c) in zip(alloc, caps))
    end
    # bias toward hpc
    _, halloc, _ = USydPhysics.allocate_workers(10, cluster, hpc, 0.9)
    @test sum(last, halloc) > 5
    # spill: hpc share exceeds hpc capacity -> cluster absorbs
    calloc, halloc, short = USydPhysics.allocate_workers(30, cluster, hpc, 0.5)
    @test sum(last, calloc) == 15 && sum(last, halloc) == 15 && short == 0
    calloc, halloc, short = USydPhysics.allocate_workers(40, cluster, hpc, 0.5)
    @test short == 10
    @test_throws ArgumentError USydPhysics.allocate_workers(10, cluster, hpc, 1.5)
end

@testset "fill_target" begin
    cluster = ["defaultQ" => 48, "taiji" => 32]
    hpc = ["orr" => 3, "karl" => 20]
    c, h, np = USydPhysics.fill_target(cluster, hpc, 0.1)
    @test c == ["defaultQ" => 43, "taiji" => 28] && h == ["orr" => 2, "karl" => 18]
    @test np == 43 + 28 + 2 + 18
    # zero buffer takes everything; empty pools give zero target
    @test USydPhysics.fill_target(cluster, hpc, 0.0)[3] == 103
    @test USydPhysics.fill_target(Pair{String, Int}[], Pair{String, Int}[], 0.1)[3] == 0
end

@testset "proportional_split" begin
    @test USydPhysics.proportional_split(0, ["a" => 5]) == ["a" => 0]
    split4 = USydPhysics.proportional_split(4, ["a" => 6, "b" => 2])
    @test sum(last, split4) == 4 && last(split4[1]) == 3 && last(split4[2]) == 1
    # respects caps
    split7 = USydPhysics.proportional_split(7, ["a" => 1, "b" => 6])
    @test split7 == ["a" => 1, "b" => 6]
end
