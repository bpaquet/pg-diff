A tool do diff Postgres databases at scale

Full list of options

  pg-diff --help

Use it with docker

  docker build . -t foo && docker run --rm -ti -v $HOME/.pgpass:/root/.pgpass foo /pg-diff <list of options>