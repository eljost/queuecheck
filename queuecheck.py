#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import re
import shutil
import sys

import spur

USERNAME = "DEIN KUERZEL"
# Regex
JOB_RE = "(\d+).*"
NODE_RE = "\s+(\w+)/.*"
# Local dir
HOME_DIR = os.path.join("DEIN HOME", USERNAME)
SAVE_DIR = os.path.join(HOME_DIR, "tmp")
# Remote dirs
WORK_DIR = "/work"

if __name__ == "__main__":
    qs = ["qstat", "-u", USERNAME, "-n"]
    # find nodes with running jobs
    local_shell = spur.LocalShell()
    r = lambda cmd: local_shell.run(cmd)
    qs_res = r(qs)

    job_ids = list()
    node_ids = list()
    match_node = False
    for line in qs_res.output.split("\n"):
        if match_node:
            # extract node id
            node_ids.append(re.match(NODE_RE, line).groups()[0])
            match_node = False
        if USERNAME in line:
            # extract job id
            job_ids.append(re.match(JOB_RE, line).groups()[0])
            # search for node in next line
            match_node = True

    if len(job_ids) is 0:
        sys.exit()

    zipped = zip(job_ids, node_ids)

    """
    for job_id, node_id in zipped:
        print("Found job with ID %r running on node %r" % (job_id, node_id))
    """
    
    for job_id, node_id in zipped:
        # Connect to remote host
        shell = spur.SshShell(hostname=node_id,
                missing_host_key=spur.ssh.MissingHostKey.accept)
        with shell:
            # Shortcut to run commands
            ssh_r = lambda cmd: shell.run(cmd)
            # List content of /work directory
            dir_res = ssh_r(["dir", WORK_DIR])
            dir_lines = dir_res.output.split("\n")
            # Find directory that starts with job id
            job_dir_rel = [l for l in dir_lines if (job_id in l)]
            if len(job_dir_rel) is 1:
                job_dir_rel = job_dir_rel[0]
            else:
                continue
            if " " in job_dir_rel:
                job_dir_rel = job_dir_rel.split()[0]
            # Create absolute path
            job_dir_abs = os.path.join(WORK_DIR, job_dir_rel)
            # List content of job directory
            job_dir_res = ssh_r(["ls", "-1", job_dir_abs])
            job_dir_content = job_dir_res.output.split("\n")
            # Find *.out file and create absolute path
            out_file_rel = [fn for fn in job_dir_content if
                    fn.endswith(".out")][0]
            out_file_abs = os.path.join(job_dir_abs, out_file_rel)
            local_path = os.path.join(SAVE_DIR, out_file_rel)
            ssh_r(["cp", out_file_abs, local_path])
            print(out_file_rel)
