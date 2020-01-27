# PyPGcmds
This is a small script to run n number of postgres commands in parallel.

```
python postgres_cmd.py host database username password role dir threads work_mem_gb

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
