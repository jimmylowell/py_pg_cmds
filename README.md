# py_pg_cmds
run lots of pg cmds at the same time

```
python py_pg_cmds.py host database username password role dir threads work_mem_gb

positional arguments:
  host         hostname to connect to
  database     DB name
  username     username to login with
  password     password to login with
  role         role to use "SET ROLE [role];"
  dir          dir that has sql files
  threads      how many threads to run
  work_mem_gb  SET work_mem = [work_memgb]GB;

optional arguments:
  -h, --help   show this help message and exit
```
