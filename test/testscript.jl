# * Simple test script to write the current date to a given file.
using Dates
file = length(ARGS) == 0 ? tempname() : first(ARGS)

mkpath(first(Base.splitdir(file)))
day = Dates.today()
open(file, "w") do io
    write(io, "$(day)\n")
end
@info "Wrote current date $(day) to file: $(abspath(file))"
