from __future__ import print_function

import os, platform, sys
import subprocess
import datetime

def import_module(name):
    __import__(name)
    return sys.modules[name]

def module_member(name):
    mod, member = name.rsplit('.', 1)
    module = import_module(mod)
    return getattr(module, member)

def run_pipeline(pipeline, pipeline_index=0, *args, **kwargs):
    """
    Runs pipeline specified as dictionary
    """

    out = kwargs.copy()

    for idx, name in enumerate(pipeline):
        print("Running " + name + "... ")
        out['pipeline_index'] = pipeline_index + idx
        func = module_member(name)
        result = func(*args, **out) or {}
        if not isinstance(result, dict):
            return result
        
        out.update(result)
    return out

def date_range(start, end, delta):
    current = start
    while current <= end:
        yield current
        current += delta

def generate_raw_filename(datetime_obj, logger_name='AIU-1309', extension='.ghg'):
    datetime_obj = datetime_obj.replace(second=0, microsecond=0)
    date = ''.join(datetime_obj.isoformat().split(':'))
    return '_'.join([date, logger_name + extension]) 

def generate_summary_filename(datetime_obj, logger_name='AIU-1309'):
    datetime_obj = datetime_obj.replace(second=0, microsecond=0) # discard decimal seconds if nec.
    date = datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
    return '_'.join([date, logger_name]) 

def list_raw_filenames_in_time_window(datetime_min, datetime_max, dt=datetime.timedelta(minutes=30)):
    dates = [x for x in date_range(datetime_min, datetime_max, dt)]
    filenames = [generate_raw_filename(d) for d in dates]
    return filenames

def list_summary_filenames_in_time_window(datetime_min, datetime_max, dt=datetime.timedelta(days=1)):
    dates = [x for x in date_range(datetime_min, datetime_max, dt)]
    filenames_summary = [generate_summary_filename(d) + '_Summary.txt' for d in dates]
    filenames_EP = [generate_summary_filename(d) + '_EP-Summary.txt' for d in dates]
    return filenames_summary + filenames_EP

def list_remote_directory(ip_address, username, private_key_file, remote_directory):
    """
    Returns incremental directory listing of directory on remote host
    """

    command = ["rsync",  "-e", "ssh -i " + private_key_file, "-anz",  username + "@" + ip_address + ":" + remote_directory]

    try:
        output = subprocess.check_output(command)
        files = [s.split(' ')[-1] for s in output.split('\n')]
        files = [s for s in files if s != '' and s != '.']
        return files
    except subprocess.CalledProcessError as error:
        print("CalledProcessError: " + error)
    except OSError as error:
        print("OSError: " + error)

def ping(host):
    """
    Returns True if host responds to a ping request
    """

    # Ping parameters as function of OS
    ping_str = " -n 1 " if  platform.system().lower()=="windows" else " -c 1 "

    # Ping
    p = subprocess.Popen("ping" + ping_str + host, shell=True)
    p.wait()

    return p.poll() == 0
