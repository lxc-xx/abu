#!/usr/bin/env python
import sys
import os
import psutil
import subprocess


def touch(fname): 
    try: 
        os.utime(fname, None) 
    except: 
        open(fname, 'a').close()

def main(argv):
    if len(argv) != 6:
        print "Usage: client.py cmds_list_path head tail parallel start_file end_file"
        sys.exit(1)

    cmds_list_path = argv[0]
    head = argv[1]
    tail = argv[2]
    parallel = argv[3]

    start_file_path = argv[4]
    end_file_path= argv[5]

    if os.path.exists(cmds_list_path):
        while True:
            touch(start_file_path)
            cmd = "head -%s %s | tail -%s | xargs -L 1 -I {} -P %s dumb.py {}" % (head, cmds_list_path, tail, parallel)
            proc = subprocess.Popen(cmd, shell=True)

            os.sleep(2)

            if proc.pid not in psutil.get_pid_list() or psutil.Process(proc.pid).status() is "zombie":
                touch(end_file_path)
                break

if __name__ == "__main__":
    main(sys.argv[1:])
