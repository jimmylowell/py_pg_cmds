import argparse
import psycopg2
import os
from multiprocessing import Pool
import datetime
from itertools import repeat


# TO DO: Convert to logging
def print_msg(error_id, msg):
    time_string = str(datetime.datetime.now())
    msg_text = time_string + " - " + error_id + ": " + msg
    print(msg_text)


### TO DO
# def get_pg_cmds_from_file(file_name):
#     # full_filename = o
#     f = open(file_name, "r")
#     file_content = f.read()
#     cmds = file_content
#     for cmd in cmds:
#         print_msg('I', 'PG command: ' + cmd)


def get_pg_cmds_from_dir(cmd_folder):
    pg_cmds = []
    for filename in os.listdir(cmd_folder):
        full_filename = os.path.join(cmd_folder, filename)
        # print_msg('I', 'FILENAME: ' + full_filename)
        f = open(full_filename, "r")
        file_content = f.read()
        # print_msg('I', 'PG from file: ' + file_content)
        pg_cmds.append(file_content)
    return pg_cmds


def run_pg_cmd(pg_cmd, args):
    cmd_start_time = datetime.datetime.now()
    try:
        # print_msg('I', 'CONNECTING TO: ' + args.host + '/' + args.database)
        conn = psycopg2.connect(host=args.host, database=args.database, user=args.username, password=args.password)
        cur = conn.cursor()
        try:
            # Role setup
            role_cmd = "SET ROLE " + args.role + ";"
            # print_msg('I', 'Setting role: ' + role_cmd)
            cur.execute(role_cmd)

            # work_mem setup
            work_mem_cmd = "SET work_mem = '" + str(args.work_mem_gb) + "GB';"
            # print_msg('I', 'Setting work_mem: ' + work_mem_cmd)
            cur.execute(work_mem_cmd)

            # Run actual command
            print_msg('I', 'Running cmd: ' + pg_cmd)
            cur.execute(pg_cmd)
            print_msg('I', 'CMD status: ' + cur.statusmessage)


            ### TO DO
            # print_msg('I', 'Result row count: ' + str(cur.rowcount))
            # result_row_count = 0
            # for row in cur:
            #     result_row_count += 1
            #     csv_row = ''
            #     for column in row:
            #         csv_row += str(column) + ', '
            #     csv_row = csv_row[:-2]
                # print_msg('I', 'Result row: ' + str(result_row_count) + ': ' + csv_row)

            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print_msg('E', 'COULD NOT RUN CMD: ' + pg_cmd)
            print_msg('E', 'PG Error: ' + error)
        finally:
            cur.close()
            conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print_msg('E', 'COULD NOT CONNECT: ' + pg_cmd)
        print_msg('E', 'PG Error: ' + error)
    finally:
        cmd_end_time = datetime.datetime.now()
        cmd_run_time = cmd_end_time - cmd_start_time
        cmd_run_time_secs = float(cmd_run_time.total_seconds())
        print_msg('I', 'CMD DONE: ' + pg_cmd)
        print_msg('I', 'Runtime (seconds): ' + str(cmd_run_time_secs))
        return cmd_run_time_secs


if __name__ == '__main__':
    # setup args
    parser = argparse.ArgumentParser(description='Run lots of postgres commands')
    parser.add_argument('host',
                        type=str,
                        # nargs=1,
                        help='hostname to connect to')
    parser.add_argument('database',
                        type=str,
                        # nargs=1,
                        help='DB name')
    parser.add_argument('username',
                        type=str,
                        # nargs=1,
                        help='username to login with')
    parser.add_argument('password',
                        type=str,
                        # nargs=1,
                        help='password to login with')
    parser.add_argument('role',
                        type=str,
                        # nargs=1,
                        help='role to use "SET ROLE [role];"')
    parser.add_argument('dir',
                        type=str,
                        # nargs=1,
                        help='dir that has sql files')
    parser.add_argument('threads',
                        # metavar='T',
                        type=int,
                        # nargs=1,
                        help='how many threads to run')
    parser.add_argument('work_mem_gb',
                        type=int,
                        # nargs=1,
                        help='SET work_mem = [work_memgb]GB;'
                        )

    args = parser.parse_args()

    start_time = datetime.datetime.now()

    # Get the list of pg_cmds for pool
    print_msg('I', 'OPENING DIR: ' + args.dir)
    pg_cmds = get_pg_cmds_from_dir(args.dir)
    total_cmds = len(pg_cmds)
    print_msg('I', 'TOTAL CMDS: ' + str(total_cmds))

    # Multi-threading business and run commands
    pool = Pool(args.threads)
    cmd_runtimes = pool.starmap(run_pg_cmd, zip(pg_cmds, repeat(args)))
    total_cmd_runtime = sum(cmd_runtimes)
    pool.close()
    pool.join()

    # Wrapup info
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    run_time_secs = float(run_time.total_seconds())

    print_msg('I', 'TOTAL CMDs RUN:' + str(total_cmds))
    print_msg('I', 'TOTAL CMD RUNTIME (seconds):' + str(total_cmd_runtime))
    print_msg('I', 'TOTAL RUNTIME (seconds):' + str(run_time_secs))
