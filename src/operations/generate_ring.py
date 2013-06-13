import sys

def generate(fileName,size):
    f = file(fileName,"w")
    for i in xrange(size):
        print >> f,"%d,%d"%(i,i+1)
        pass
    print >> f,"%d,0"%(size)
    f.flush()
    f.close()
    pass

if __name__ == "__main__":
    generate(sys.argv[1],int(sys.argv[2]))
    pass
    
    
