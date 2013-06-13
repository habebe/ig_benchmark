import os
import sys
import base
import platform
import shutil
import subprocess

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        pass

    def run_make(self,base_dir):
        cwd = os.getcwd()
        os.chdir(base_dir)
        arguments = ['make']
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        pass

    def run(self):
        platform_name = platform.uname()[0].lower()
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        graph_dir = os.path.join(base_dir,"graph500-2.1.4")
        make_inc_dir = os.path.join(graph_dir,"make-incs")
        make_file = None
        if platform_name.startswith("darwin"):    
            make_file = os.path.join(make_inc_dir,"make.inc-osx")
        elif platform_name.startswith("linux"):
            make_file = os.path.join(make_inc_dir,"make.inc-gcc")
            pass
        shutil.copyfile(make_file,os.path.join(graph_dir,"make.inc"))
        self.run_make(graph_dir)
        return True
    

    def operate(self):
        if base.operation.operate(self):
            self.run()
        return False
