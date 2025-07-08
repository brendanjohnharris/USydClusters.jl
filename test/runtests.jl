using USydClusters
using Test

@testset "USydClusters.jl" begin
    if haskey(ENV, "JULIA_DISTRIBUTED") && ENV["JULIA_DISTRIBUTED"]
        include("test/physics_test.jl")
    end
end
