using AcademicClusters
using Test


@testset "USydPhysics" begin
    if contains(gethostname(), "physics.usyd.edu.au")
        include("USydPhysics/distributeprocs_test.jl")
        include("USydPhysics/physics_test.jl")
    end
end

@testset "NCIGadi" begin
    include("NCIGadi/gadi_unit_test.jl")
    if contains(gethostname(), "gadi")
        include("NCIGadi/gadi_test.jl")
    end
end
