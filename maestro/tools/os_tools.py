import os, sys, subprocess

def check_pid(process_id):
    if sys.platform == "win32":
        pids = []
        tasklist = subprocess.Popen(["tasklist"], stdout=subprocess.PIPE)
        try:
            for task in tasklist.communicate()[0].split("\r\n"):
                try:
                    pids.append(int(task[26:34]))
                except:
                    pass
        except Exception as e:
            raise OSError("Unable to list process ids: " + str(e))
        if int(process_id) in pids:
            return True
        else:
            return False
    else:
        try:
            os.kill(process_id, 0)
        except OSError:
            return False
        else:
            return True

