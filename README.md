# Key To Redis output plugin for Embulk

Generate the aggregated key from input values and output to Redis's SET value.

This plugin is designed to extract data set diff files used with the combination in below use cases.

1. Use this plugin and output specified key's to redis.
    - this plugin.
2. Input another data source and use filter key_in_redis plugin with specified key's then filtered the key's (or that hash).  
    - https://github.com/smdmts/embulk-filter-key_in_redis

 
## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration


| name                                 | type        | required?  | default                  | description            |  
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  host                                | string      | optional   | "127.0.0.1"              | redis servers host     |
|  port                                | integer     | optional   | "6379"                   | redis servers port     |
|  db                                  | integer     | optional   | "null"                   | redis servers db       |
|  flush_on_start                      | boolean     | optional   | "false"                  | flush on start specified redis servers db |
|  redis_set_key                       | string      | required   |                          | redis of key of set name |
|  appender                            | string      | optional   | "-"                      | multi key of appender  |
|  put_as_md5                          | boolean     | optional   | "false"                  | sadd the value to converted md5 |
|  key_with_index                      | hash: Map<Int,String> | required with key_with_index or json_key_with_index or only one || index with key name |
|  json_key_with_index                 | hash: Map<Int,String> | required with key_with_index or json_key_with_index or only one || json columns's expanded key name |
|  default_timezone                    | string      | optional   | UTC                      | |
|  default_timestamp_format            | string      | optional   | %Y-%m-%d %H:%M:%S.%6N    | |

## Example

- input json
```json
{  
   "device_id":"ABC",
   "timestamp_micros":1502590079312009,
   "params":{  
      "UserID":"user_id_12345"
   }
}
```

- definition's yaml
```yaml
filters:
  - type: expand_json
    json_column_name: record
    root: "$."
    stop_on_invalid_record: false
    expanded_columns:
      - { name: "device_id", type: string }
      - { name: "timestamp_micros", type: long }
      - { name: "params", type: json }
out:
  type: "key_to_redis"
  redis_set_key: redis_key
  flush_on_start: true
  put_as_md5: false
  appender: "_"
  key_with_index: 
    1: "device_id"
    2: "timestamp_micros"
  json_key_with_index:
    3: "UserID" 
```

- output as redis command
```
sadd redis_key "ABC_1502590079312009_user_id_12345"
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
