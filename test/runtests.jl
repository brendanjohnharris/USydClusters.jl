using USydClusters
using Test

@testset "distributeprocs" begin
    include("distributeprocs_test.jl")
end

@testset "USydPhysics" begin
    if haskey(ENV, "JULIA_DISTRIBUTED") && ENV["JULIA_DISTRIBUTED"] == "true"
        include("physics_test.jl")
    end
end
