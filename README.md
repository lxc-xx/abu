![alt text](https://raw.githubusercontent.com/lxc-xx/abu/master/abu_lamp.gif)


[Image source](http://www.disneyclips.com/imagesnewb/aladdin-abu.html)

# abu
An AWS Cluster Manager

##Install

```
pip install abu
```
###Usage Example 
```python
#!/usr/bin/env python
#import Abu
from abu.abu import Abu

if __name__ == "__main__":
    #specify AWS region
    region = 'us-east-1'
    #specify your key file path on AMI
    key_file_path = '/home/ubuntu/xiaojun_nfs.pem'
    #specify your key ID
    key_name = 'xiaojun_east'
    #specify your AMI ID
    ami_id = 'ami-32bd525f'
    #specify your security group
    security_group = 'NFS'
    #specify the number of instances you want to use
    inst_num = 3

    #the instance type you want to launch
    inst_type = 't2.micro'

    #Your NFS address and path
    nfs_mount_dict = {'host':'nfs_host_address', 'mount_dirs': ['/home/ubuntu/Data_ex1', '/home/ubuntu/Data_ex2','/home/ubuntu/Data_ex3']}

    #The path for hearts files, It should be accessible by spawned instances
    hearts_path = '/home/ubuntu/Data_ex1/hearts'

    #Name your job
    clust_name = 'abu_test'

    #Put a path of client.py. It should be accessible by spawned instances
    client_path = "/home/ubuntu/anaconda/lib/python2.7/site-packages/abu/client.py"

    abu = Abu(key_file_path=key_file_path,
            key_name=key_name,
            security_group=security_group,
            ami_id=ami_id,
            region=region,
            inst_num=inst_num,
            inst_type=inst_type,
            nfs_mount_dict=nfs_mount_dict,
            hearts_path=hearts_path,
            client_path=client_path,
            clust_name = clust_name,
            terminate_on_del=True)

    abu.init_insts()

    job_name = "sleep_test"
    cmds_list_path = "/home/ubuntu/Data_ex1/xcli/temp/cmds.sh"

    #number of parallel processes per instance you want to use
    proc_per_instance = 3

    #commands per job
    cmds_per_job = 3

    log_dir = '/home/ubuntu/Data_ex1/xcli/temp/log'
    err_dir = '/home/ubuntu/Data_ex1/xcli/temp/err'
    done_dir = '/home/ubuntu/Data_ex1/xcli/temp/done'

    abu.run(job_name, cmds_list_path, proc_per_instance, cmds_per_job, log_dir, err_dir, done_dir)
```
