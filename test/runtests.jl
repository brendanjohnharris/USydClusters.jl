using USydClusters
using Test

@testset "Physics" begin
    if haskey(ENV, "JULIA_DISTRIBUTED") && ENV["JULIA_DISTRIBUTED"] == "true"
        include("physics_test.jl")
    end
end
