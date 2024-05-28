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
* `key_stop`: With `by_id` and `by_timestamp` strategy, specify where to start instead of using `max`.
* `limit_to_the_past_minutes` and `limit_to_the_past_key`: useful when you comparing tables synchronized through an async pipeline (like Kafka / Debezium). Do not compare the x last minutes, based on the specified key (`created_at` for example)

## Extract the result to reuse them

* `record_sql_file`: It will output all the difference in a file to allow reuse of the difference (for example to resync them).

Format:
```
changed: 12
only_in_source: 14
only_in_target: 15
```

The number is the first item of each row, which is the primary key is most of tables.

## Run it in docker

    docker build . -t foo && docker run --rm -ti -v $HOME/.pgpass:/root/.pgpass foo /pg-diff <list of options>
