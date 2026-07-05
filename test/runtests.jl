using AcademicClusters
using Test


@testset "USydPhysics" begin
    if contains(hostname(), "physics.usyd.edu.au")
        include("USydPhysics/distributeprocs_test.jl")
        include("USydPhysics/physics_test.jl")
    end
end
