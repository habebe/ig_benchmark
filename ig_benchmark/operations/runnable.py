import base

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        pass

    def is_runnable(self):
        return True

    def run(self,db,suite,case,data,**kwargs):
        assert 0

    
