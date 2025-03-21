# A tool do diff Postgres databases at scale.

At scale, but written in Ruby? Yes, because the diff is not performed by the ruby code, but by battle-test tools written in C:
* `psql` to retrieve the data
* `diff`, which process the two data flow
* UNIX pipes to connect them

Ruby is here only for orchestration: split the work to do and launch low level tools in parallel.

## Strategy

3 strategies can be used
* `one_shot`: The table will be fully downloaded and compared in one stage. Works only with small tables.
* `by_id`: Split the work to do using a numerical id. Compute the `min` and the `max`, and create batches of work of `batch_size` rows. Default batch_size is 10000. Not efficient with table which have holes in id sequences.
* `by_timestamp`: Same as id, but with a `timestamp` field, like `created_at` or `updated_at`. Batch size unit is days, default value is 10 days.

Note: on real table, you need an index on the column used as `key`.

## Compare only a subset of the dataset

* `key_start`: With `by_id` and `by_timestamp` strategy, specify where to start instead of using `min`.
* `key_stop`: With `by_id` and `by_timestamp` strategy, specify where to start instead of using `max`. Relative value to now can be used: `now-5` means now minus 5 seconds.

## Extract the result to reuse them

* `extract_result_to_file`: It will output all the difference in a file to allow reuse of the difference (for example to resync them).

Format:
```
changed: 12
only_in_source: 14
only_in_target: 15
```

The number is the first item of each row, which is the primary key is most of tables.

## Use custom_select

`pg-diff` generate by default queries like `select a, b, c from ....`.

You can use `--custom_select='count(*)'` to generate `select count(*) from ...`. This can be really useful for append only table.

## Recheck for errors after x seconds

`--recheck_for_errors` will recheck the lines in errors after x seconds.

The recheck is performed using the first item of each row. This is especially useful
while diffing data on a live streaming replication.

## Run it in docker

    docker build . -t foo && docker run --rm -ti -v $HOME/.pgpass:/root/.pgpass foo /pg-diff <list of options>
