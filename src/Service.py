import socket
import sys
import operations
import threading

def GetHostName():
    return socket.gethostname()

def IsLocalAddress(address):
    if address == "127.0.0.1":
        return True
    hostname = GetHostName()
    if address == hostname:
        return True
    try:
        resolved = socket.gethostbyname(address)
        local = socket.gethostbyname(hostname)
        if local == resolved:
            return True
        pass
    except:
        pass
    return False
    

class Server:
    DEFAULT_PORT = 50000
    def init(self):
        self.socket = None
        print "\tCreating server on host:{0} port:{1}".format(self.host,self.port)
        for res in socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE):
            af, socktype, proto, canonname, sa = res
            try:
                self.socket = socket.socket(af, socktype, proto)
            except socket.error, msg:
                self.socket = None
                continue
            try:
                print "\tBinding",canonname
                self.socket.bind(sa)
                print "\tListening",canonname
                self.socket.listen(1)
            except socket.error, msg:
                self.socket.close()
                self.socket = None
                continue
            break
        return (self.socket != None)

    def close(self):
        if self.socket:
            print "\tClosing server"
            self.socket.close()
            pass
        pass

    def recv_data(self,connection,address):
        data = ""
        done = False
        while not done:
            rdata = connection.recv(1024)
            if rdata:
                data = data + rdata
                pass
            
            if (rdata == None) or len(rdata) < 1024:
                done = True
                pass
            pass
        print '\t\tIncoming {0}:{1}'.format(address,data)
        return data

    def cmd_cb_quit(self,data):
        self.terminate = True
        return True

    def cmd_cb_operation(self,data):
        if data.has_key("name"):
            op = data["name"]
            object = operations._operation_(op)
            if object == None:
                print "Error: Unknown operation {0}".format(op)
                return False
            if data.has_key("args"):
                object.parse(data["args"])
                pass
            result = object.operate()
            return result
        else:
            error =  "Error: Operation name is not given"
            print error
            return (False,error)
        return False

    def execute_command(self,data):
        try:
            data     = eval(data)
            cmd      = None
            cmd_data = None
            if data.has_key("cmd"):
                cmd = data["cmd"]
                pass
            if data.has_key("data"):
                cmd_data = data["data"]
                pass
            if cmd:
                if self.cmd_map.has_key(cmd):
                    return self.cmd_map[cmd](cmd_data)
                else:
                    print "Unknown cmd:{0}".format(cmd)
                    pass
            return False
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message = "\t\t{0} {1} ".format(str(exc_type),exc_obj)
            print message
            pass
        return False
    
    def run(self):
        if self.socket != None:
            pair = self.socket.accept()
            while pair is not None:
                (connection,address) = pair
                print '\tConnection {0}'.format(address)
                data = self.recv_data(connection,address)
                status = self.execute_command(data)
                try:
                    print "\t\tOutgoing",str(status)
                    connection.send(str(status))
                except:
                    pass
                if not self.terminate:
                    pair = self.socket.accept()
                else:
                    pair = None
                pass
            pass
        self.close()
        pass

    def __init__(self,port=DEFAULT_PORT):
        self.socket = None
        self.port = port
        self.host = socket.gethostname()
        self.terminate = False
        self.cmd_map = {
            "quit":self.cmd_cb_quit,
            "operation":self.cmd_cb_operation
            }
        pass
    pass


class Request(threading.Thread):
    def __init__(self,host,port=Server.DEFAULT_PORT):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.requestOptions = None
        self.response = None
        pass


    def init(self):
        print self.host,self.port
        for res in socket.getaddrinfo(self.host,self.port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                self.socket = socket.socket(af, socktype, proto)
            except socket.error, msg:
                self.socket = None
                print "Socket Error:",msg,canonname
                continue
            try:
                self.socket.connect(sa)
                print "\t\t\tRemote Request connected {0}".format(sa)
            except socket.error, msg:
                self.socket = None
                print "Connect Error:",msg,sa
                continue
            break
        return (self.socket != None)

    def stop_service(self):
        if self.socket:
            self.socket.send(str({"cmd":"quit"}))
            data = self.socket.recv(1024)
            return eval(data)
        return None

    def request(self,r,data):
        self.requestOptions = {"cmd":"operation","data":{"name":r,"args":data}} 
        pass

    def run(self):
        if self.requestOptions and self.socket:
            print "\t\t\tRemote Request Sending"
            print "\t\t\t",self.requestOptions
            self.socket.send(str(self.requestOptions))
            self.response = self.socket.recv(1024)
            return eval(self.response)
        return
            
    
    def close(self):
        if self.socket:
            self.socket.close()
            pass
        pass
    pass


