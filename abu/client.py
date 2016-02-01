#!/usr/bin/env python
import sys
import os
import psutil
import subprocess
import time


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
        touch(start_file_path) 
        cmd = "head -%s %s | tail -%s | parallel -j %s" % (head, cmds_list_path, tail, parallel) 
        proc = subprocess.Popen(cmd, shell=True, executable="/bin/bash") 
        proc.wait() 
        touch(end_file_path)

if __name__ == "__main__":
    main(sys.argv[1:])
