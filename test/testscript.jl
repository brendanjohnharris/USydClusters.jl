# * Simple test script to write the current date to a given file.
using UUIDs

file = length(ARGS) == 0 ? tempname() : first(ARGS)
key = length(ARGS) > 1 ? ARGS[2] : string(UUIDs.uuid4())

mkpath(first(Base.splitdir(file)))
open(file, "w") do io
    write(io, "$(key)")
end
print("$(abspath(file)): $key")
