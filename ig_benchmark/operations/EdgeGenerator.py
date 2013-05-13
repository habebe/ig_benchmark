import sys

class Generate:
    def __init__(self,fileName,connections,max_depth,limit=-1):
        self.counter = 0
        self.connections = connections
        self.max_depth = max_depth
        self.limit = limit
        self.file = file(fileName,"w")
        pass
    
    def getNumber(self,add=True):
        if add:
            self.counter += 1
            pass
        return self.counter

    def run(self):
        self.generate(self.file,0,0,self.connections,
                      self.max_depth,self.limit)
        self.file.flush()
        self.file.close()
        pass
    
    def generate(self,f,start,depth,connections,max_depth,limit=-1):
        if depth == max_depth:
            return
        numbers = []
        for i in xrange(connections):
            numbers.append(self.getNumber(True))
            pass
        for i in numbers:
            s = "%d,%d"%(start,i)
            print >> f,s
            pass
        if limit < 0:
            for i in numbers:
                self.generate(f,i,depth+1,connections,max_depth,limit)
                pass
            pass
        else:
            if self.getNumber(False) < limit:
                for i in numbers:
                    self.generate(f,i,depth+1,connections,max_depth,limit)
                    pass
                pass
            pass
       
       
        pass
    
    def calculate(self,connection,depth):
        s = -1
        for i in xrange(depth+1):
            s = s + pow(connection,i)
            pass
        return s


#generate(f,0,0,int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]))
#print "Vertices:",Counter

if __name__ == "__main__":
    generator = Generate("output.txt",
                         int(sys.argv[1]),
                         int(sys.argv[2]),
                         int(sys.argv[3]))
    generator.run()
    print generator.getNumber(False)
    pass

#,calculate(int(sys.argv[1]),int(sys.argv[2]))


    
