import subprocess
import os

def run(host):
    stdout_name = "__temp__oocheckls__stdout__"
    stderr_name = "__temp__oocheckls__stderr__"
    f1 = file(stdout_name,"w")
    f2 = file(stderr_name,"w")
    status = None
    command = ["oocheckls"]
    if host:
        command.append("host")
        pass
    status = subprocess.call(command,stdout=f1,stderr=f2)
    f1.close()
    f2.close()
    f1 = file(stdout_name,"r")
    f2 = file(stderr_name,"r")
    output1 = f1.read()
    output2 = f2.read()
    f1.close()
    f2.close()
    os.remove(stdout_name)
    os.remove(stderr_name)
    return (status,output1.split("\n"),output2.split("\n"))
    
