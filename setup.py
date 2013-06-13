#!/usr/bin/env python

import sys
import os

def get_file_list(wd,rootdir):
    cwd = os.getcwd()
    os.chdir(wd)
    fileList = []
    for root, subFolders, files in os.walk(rootdir):
        for file in files:
            fileList.append(os.path.join(root,file))
            pass
        pass
    os.chdir(cwd)
    return fileList

from distutils.core import setup

try:
    import ig_template
except:
    print >> sys.stderr,"Error: ig_template project need to installed first."
    sys.exit(1)
    pass


setup(name='ig_benchmark',
      version='Beta',
      description='InfiniteGraph Benchmark',
      author='Henock Abebe',
      author_email='habebe@objectivity.com',
      url='http://www.objectivity.com',
      packages=['ig_benchmark'],
      package_dir={'ig_benchmark': 'src'},
      package_data={'ig_benchmark':get_file_list("src","web_template")},
      scripts=['src/ig.benchmark.py'],
      )

#['web_template/css/data_tables/*','web_template/css/smoothness/*','web_template/css/smoothness/images/*','web_template/*']},
