using USydClusters
using Documenter

DocMeta.setdocmeta!(USydClusters, :DocTestSetup, :(using USydClusters); recursive=true)

makedocs(;
    modules=[USydClusters],
    authors="brendanjohnharris <brendanjohnharris@gmail.com> and contributors",
    repo="https://github.com/brendanjohnharris/USydClusters.jl/blob/{commit}{path}#{line}",
    sitename="USydClusters.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://brendanjohnharris.github.io/USydClusters.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/brendanjohnharris/USydClusters.jl",
    devbranch="main",
)
