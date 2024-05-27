A tool do diff Postgres databases at scale.

No diff is performed in ruby. The diff by itself id one through `psql` process piped to the standard UNIX `diff`. Ruby is here only to orchestrate processes.

Full list of options

    pg-diff --help

Use it with docker

    docker build . -t foo && docker run --rm -ti -v $HOME/.pgpass:/root/.pgpass foo /pg-diff <list of options>
