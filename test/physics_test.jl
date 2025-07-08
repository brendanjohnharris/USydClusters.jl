using USydClusters
script = abspath("test/testscript.jl")
tempfile = abspath("./test/test.txt")
logfile = USydClusters.Physics.runscript(script; ncpus = 1, mem = 1, walltime = 1,
                                         args = tempfile)
