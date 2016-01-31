import os
import sys
import time
import boto
import boto.ec2
from datetime import datetime
import commands
from enum import Enum
import time

def enum(**enums): 
    return type('Enum', (), enums)

class JobStatus(Enum): 
    RUNNING=0
    FINISHED=1
    DEAD=2
    UNSTARTED=3

class InstStatus(Enum): 
    RUNNING=0
    IDLE=1
    INITIALIZING=2
    DEAD=3
    UNSTARTED =4

class AWSInstance(object):
    def __init__(self, inst_id, hearts_path, tolerance = 50, ip = None, job_id=None):
        self.inst_id = inst_id
        self.heart_file_path = os.path.join(hearts_path, inst_id + ".hrt")
        self.tolerance = tolerance
        self.warning_count = 0
        self.last_signal = 0
        self.status = InstStatus.UNSTARTED
        self.ip = ip

        self.patience = 100
        self.timer = time.time()

        self.job_id = job_id

    def update(self, abu):

        if self.status is not InstStatus.DEAD:
            if self.status is InstStatus.UNSTARTED:
                stats = abu.conn.get_all_instance_status([self.inst_id])
                if stats: 
                    self.status = InstStatus.INITIALIZING
                else:
                    timer = time.time()
                    wait = timer - self.timer

                    if wait > self.patience:
                        self.status = InstStatus.DEAD

            elif self.status is InstStatus.INITIALIZING:
                stats = abu.conn.get_all_instance_status([self.inst_id])
                if not stats:
                    self.status = InstStatus.DEAD
                else: 
                    stat = stats[0]
                    if str(stat.system_status) == "Status:ok" and str(stat.instance_status) == "Status:ok": 
                        #self.mount_nfs(abu)
                        #self.start_heart_beat(abu)
                        self.get_ip(abu)
                        self.mount_nfs(abu)
                        self.start_heart_beat(abu)
                        self.status = InstStatus.IDLE
                    else: 
                        self.status = InstStatus.INITIALIZING
            elif self.status is InstStatus.RUNNING or self.status is InstStatus.IDLE:
                if not self.is_alive():
                    self.status = InstStatus.DEAD
                    if self.job_id in abu.job_pool:
                        abu.job_pool[self.job_id].inst_id = None
                        abu.job_pool[self.job_id].status = JobStatus.DEAD
                    self.job_id = None

    def terminate(self, abu):
        if abu.conn: 
            abu.conn.terminate_instances([self.inst_id])

    def get_ip(self, abu):
        resv = abu.conn.get_all_instances([self.inst_id])[0] 
        inst = resv.instances[0] 
        self.ip = inst.public_dns_name

    def is_alive(self):
        #stat_heart_cmd = "stat " + self.heart_file_path + "| grep Modify"
        #err,new_signal = commands.getstatusoutput(stat_heart_cmd)

        #print "heart beat on " + self.inst_id  + ": " + new_signal

        if os.path.exists(self.heart_file_path):
            new_signal = os.path.getmtime(self.heart_file_path)
            if new_signal == self.last_signal:
                self.warning_count += 1
            else:
                self.last_signal = new_signal
                self.warning_count = 0
            print "heart file exists on " + self.inst_id  + " mtime: " + str(new_signal)
        else:
            print "heart file not exists on " + self.inst_id
            self.warning_count += 1

        print "warning_count on " + self.inst_id + " is " + str(self.warning_count)

        if self.warning_count > self.tolerance:
            return False
        else:
            return True

    def mount_nfs(self, abu):
        print "Mount NFS on " + self.ip
        nfs_cmd = abu.gen_nfs_cmd()
        self.run_cmd(nfs_cmd, abu)

    def start_heart_beat(self, abu, rate=1):
        print "Start heart beat on " + self.ip
        heart_cmd = "(while true; do touch "+self.heart_file_path+";sleep "+str(rate)+"s; done&)"
        self.run_cmd(heart_cmd, abu)

    def run_cmd(self, cmd, abu, log_path = "/dev/null", err_path = "/dev/null", done_path = "/dev/null"):
        ssh_cmd = abu.gen_ssh_cmd(cmd, self.ip, log_path=log_path, err_path=err_path, done_path=done_path)
        abu.abu_execute(ssh_cmd)

class AWSJob(object):
    def __init__(self, job_id,  cmd, log_file, err_file, done_file, start_file, end_file, inst_id=None):
        self.job_id = job_id
        self.cmd=cmd
        self.status = JobStatus.UNSTARTED
        self.log_file = log_file
        self.err_file = err_file
        self.done_file = done_file
        self.inst_id = inst_id
        self.start_file = start_file
        self.end_file = end_file

        self.start_tolerance = 50
        self.start_cnt = 0

    def update(self,abu):
        if self.status is JobStatus.UNSTARTED:
            #find a idle instance to run the job
            for inst_id in abu.inst_ids:
                inst = abu.insts_pool[inst_id]
                if inst.status is InstStatus.IDLE:
                    inst.run_cmd(self.cmd, abu, log_path=self.log_file, err_path=self.err_file,done_path=self.done_file)
                    inst.status=InstStatus.RUNNING
                    inst.job_id = self.job_id
                    self.status=JobStatus.RUNNING
                    self.inst_id = inst_id
                    break
            if self.status is JobStatus.UNSTARTED:   
                #wait
                pass
        elif self.status is JobStatus.RUNNING:
            #check if finished
            is_done = os.path.isfile(self.done_file)
            is_start = os.path.isfile(self.start_file)
            is_end = os.path.isfile(self.end_file)

            if is_start:
                self.start_cnt = 0
                if is_done:
                    self.status = JobStatus.FINISHED
                    inst = abu.insts_pool[self.inst_id]
                    inst.status = InstStatus.IDLE
                    inst.job_id = None
                    self.inst_id = None
            else:
                print "start cnt: " + str(self.start_cnt) + " Job start file " + self.start_file + " does not exist"
                self.start_cnt += 1

                if self.start_cnt > self.start_tolerance:
                    self.status = JobStatus.DEAD
                    inst = abu.insts_pool[self.inst_id]
                    inst.status = InstStatus.DEAD
                    inst.job_id = None
                    self.inst_id = None
                    self.start_cnt = 0
        elif self.status is JobStatus.DEAD:
            self.status = JobStatus.UNSTARTED
        elif self.status is JobStatus.FINISHED:
            pass
        else:
            pass

class Abu(object):

    def __init__(self, key_file_path, key_name, security_group, ami_id, region, inst_num, inst_type, nfs_mount_dict, hearts_path, client_path, clust_name = "abu_test", terminate_on_del=True):
        self.key_file_path = key_file_path
        self.key_name=key_name
        self.security_group=security_group
        self.ami_id=ami_id
        self.region=region
        self.inst_num=inst_num
        self.inst_type=inst_type
        self.nfs_mount_dict=nfs_mount_dict
        self.conn = boto.ec2.connect_to_region(region)
        self.hearts_path =hearts_path
        self.client_path = client_path

        self.inst_ids = []
        self.insts_pool = dict()

        self.job_ids = []
        self.job_pool = dict()

        self.clust_name = clust_name
        self.terminate_on_del=terminate_on_del



    def __del__(self):
        if self.terminate_on_del: 
            self.terminate_instance()

    def gen_nfs_cmd(self):
        mount_cmd = ";".join([" (sudo mount " + self.nfs_mount_dict['host'] + ":" + directory + " " + directory + ")" for directory in self.nfs_mount_dict['mount_dirs']])
        return mount_cmd

    def gen_ssh_cmd(self,cmd, host, log_path = "/dev/null", err_path = "/dev/null", done_path = "/dev/null"):
        logging = '> %s 2> %s' % (log_path, err_path)
        sshArgs = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i " + self.key_file_path + " ubuntu@" + host
        return 'ssh %s \"set -o pipefail && bash -ic \\"(%s ; echo $? > %s) \\"\" </dev/null %s &' % (sshArgs, cmd, done_path, logging)

    @classmethod
    def abu_execute(cls,cmd):
        sys.stderr.write( "Abu executes: " + cmd+"\n") 
        res = os.system(cmd) 
        if res != 0: 
            sys.stderr.write( "Command failed: " + cmd + "\n") 


    def new_instance(self):
        resv = self.conn.run_instances(self.ami_id, key_name=self.key_name, instance_type=self.inst_type, security_groups=[self.security_group]) 
        inst = resv.instances[0] 
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.conn.create_tags([inst.id], {"Name": "{%s %s}" % (self.clust_name,start_time) })

        return inst.id


    def init_insts(self, init_insts = None):

        if init_insts:
            for new_inst_id in init_insts:
                print "Initializing from instgance: " + new_inst_id
                new_inst = AWSInstance(new_inst_id, self.hearts_path)
                self.inst_ids.append(new_inst_id)
                self.insts_pool[new_inst_id] = new_inst
        else:
            for i in range(self.inst_num): 
                new_inst_id  = self.new_instance()
                self.inst_ids.append(new_inst_id)
                print "Creating instgance: " + new_inst_id
                new_inst = AWSInstance(new_inst_id, self.hearts_path)
                self.insts_pool[new_inst_id] = new_inst

        while True: 
            
            sys.stderr.write( "=== Instances Initialization Check %s === " % datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"\n") 
            self.update_instances()
            time.sleep(5)

            still_initing = False
            for inst_id in self.inst_ids: 
                inst = self.insts_pool[inst_id]
                if inst.status is not InstStatus.IDLE:
                    still_initing = True

            if not still_initing:
                break

    def update_instances(self):
        to_delete = []
        for inst_id in self.inst_ids:
            inst = self.insts_pool[inst_id]
            inst.update(self)

            print inst_id + ":" + inst.status.name + " is running job: " + str(inst.job_id)
            if inst.status is InstStatus.DEAD:
                inst.terminate(self)
                to_delete.append(inst_id)

        for inst_id in to_delete:
            self.inst_ids.remove(inst_id)
            self.insts_pool.pop(inst_id)
                
            #Start new instance to replace dead one
            new_inst_id  = self.new_instance()
            self.inst_ids.append(new_inst_id)
            print "Creating instgance: " + new_inst_id
            new_inst = AWSInstance(new_inst_id, self.hearts_path)
            self.insts_pool[new_inst_id] = new_inst

    def update_jobs(self):
        for job_id in self.job_pool:
            job = self.job_pool[job_id]
            job.update(self)

        unstarted_jobs = []
        running_jobs = []
        dead_jobs = []
        finished_jobs = []

        for job_id in self.job_pool:
            job = self.job_pool[job_id]
            if job.status is JobStatus.UNSTARTED:
                unstarted_jobs.append(job_id)
            elif job.status is JobStatus.RUNNING:
                running_jobs.append(job_id)
            elif job.status is JobStatus.DEAD:
                dead_jobs.append(job_id)
            elif job.status is JobStatus.FINISHED:
                finished_jobs.append(job_id)
            else:
                pass

        print "Unstarted Jobs: " + ",".join(unstarted_jobs)
        print "Running Jobs: " + ",".join(running_jobs)
        print "Dead Jobs: " + ",".join(dead_jobs)
        print "Finished Jobs: " + ",".join(finished_jobs)


    def terminate_instance(self):
        if self.conn:
            for inst_id in self.inst_ids: 
                self.insts_pool[inst_id].terminate(self)
                print "Terminating: " + inst_id

            self.conn.close()

    def run(self,job_name, cmds_list_path, proc_per_instance, jobs_per_instance, log_dir, err_dir, done_dir, max_hour=24): 

        now = str(datetime.now().strftime("%B_%d_%Y_%H_%M_%S"))
        #Creat log/err/done

        if not os.path.isdir(os.path.join(done_dir , now)): 
            os.makedirs(os.path.join(done_dir , now))
        if not os.path.isdir(os.path.join(log_dir , now)): 
            os.makedirs(os.path.join(log_dir , now))
        if not os.path.isdir(os.path.join(err_dir , now)): 
            os.makedirs(os.path.join(err_dir , now))

        cmds_list_path = os.path.abspath(cmds_list_path)
        with open(cmds_list_path) as f:
            line_num = len(f.readlines())

        #Generate paralle cmds
        start = 0
        cmd_idx = 0

        self.job_ids = []
        self.job_pool = dict()

        while start < line_num: 
            head_num = min(start + jobs_per_instance, line_num) 
            tail_num = head_num - start

            job_id = job_name + "-" + str(cmd_idx)

            log_file = os.path.join(log_dir , now , job_id + ".log")
            err_file = os.path.join(err_dir , now , job_id + ".err")
            done_file = os.path.join(done_dir , now ,  job_id + ".done")

            start_file = os.path.join(log_dir , now , job_id + ".start")
            end_file = os.path.join(log_dir , now , job_id + ".end")

            crop_cmd = "head -" + str(head_num) + " " + cmds_list_path + " | tail -" + str(tail_num) 
            start = head_num 
            xargs_cmd = "xargs -L 1 -I {} -P " + str(proc_per_instance) + " dumb.py {}" 

            time_limit_cmd = "(sleep " + str(max_hour) + "h && sudo halt&)" 

            client_cmd = ' '.join([self.client_path, cmds_list_path, str(head_num), str(tail_num), str(proc_per_instance), start_file, end_file])
            #final_cmd = time_limit_cmd + "; " + crop_cmd + " | " + xargs_cmd 
            final_cmd = time_limit_cmd + "; " + client_cmd
            self.job_ids.append(job_id)
            self.job_pool[job_id] = AWSJob(job_id, final_cmd, log_file, err_file, done_file, start_file, end_file)
            cmd_idx += 1


        while True:
            self.update_jobs()
            self.update_instances()

            finished_cnt = 0
            for job_id in self.job_pool:
                job = self.job_pool[job_id]
                if job.status is JobStatus.FINISHED:
                    finished_cnt += 1

            if finished_cnt == len(self.job_ids):
                break

            time.sleep(5)
