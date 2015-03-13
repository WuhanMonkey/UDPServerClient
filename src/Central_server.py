#cs425 mp1
import socket
import threading
import sys
import Queue
from threading import Thread
import time

heldAcks = {}   # Hashtable which is var : list(count, orginalClient, client1, client2, etc.)

class Central_server(object):
        def __init__(self, input):        
            self.s_listen=None #socket_listen
            self.s_send=None#socket_send
            self.p=None #port number
            self.h=None #HOST
            self.t_listen=None #thread_listen
            self.t_send=None #thread_send
            self.server_list=None
            self.server_a=None
            self.server_b=None
            self.server_c=None
            self.server_d=None
            self.model=input
            #self.counter_a=0
            #self.counter_b=0
            #self.counter_c=0
            #self.counter_d=0
            print "Central Server> configured to use model:", self.model
            self._Queue= Queue.Queue()
            try:
                file=open("config.txt", 'r')
            except IOError:
                print "Error: can\'t find Configuration file"         
            else:
                port = file.readline()    
                port = port.split("=")
                port = port[1].strip()
                self.p = port
                print "Central Server> The Central Server had been configured to Port:",port
                max_delay = file.readline()    
                host = file.readline()    
                host = host.split("=")
                host = host[1].strip()
                self.h = host
                print "Central Server> The Central Server had been configured to host:",host
                servers = file.readline()
                servers = servers.split("=")
                server_list = servers[1]
                server_list = server_list.split(",")
                self.server_list=server_list
                self.server_a=server_list[0]
                self.server_b=server_list[1]
                self.server_c=server_list[2]
                self.server_d=server_list[3]
                print "Central Server> The port of server_a is:",self.server_a
                print "Central Server> The port of server_b is:",self.server_b
                print "Central Server> The port of server_c is:",self.server_c
                print "Central Server> The port of server_d is:",self.server_d

                admin_msg = "admin_model"
                admin_msg = admin_msg +' '+ self.model+' '+self.p
                #below we send the model information to all the servers and wait for the ack.
                self.s_listen=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_send=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_listen.bind(('',int(self.p)))
                
                for s in server_list:
                    self.s_send.sendto(admin_msg,(self.h, int(s)))
                ack_counter = 4
                while(ack_counter!=0):
                    print "Central Server> Central Server is acknowledging the sub servers..."
                    msg, addr = self.s_listen.recvfrom(1024)
                    if not msg:
                        continue
                    msg=msg.split(" ")

                    if (msg[0] == 'ack'):
                        ack_counter=ack_counter-1
                print "Central Server> Server Acknowledged. Central server is ready to use."
                
        
        def start(self):       
            self.t_listen=threading.Thread(target=self.listen)
            self.t_listen.start()
            self.t_send=threading.Thread(target=self.send)
            self.t_send.start()          
        
        def listen(self):
            while True:
                message, addr = self.s_listen.recvfrom(1024)
                print message
                if not message:
                    continue
                #handle acks probably
                #here the addr might be the probably, we could send the address via message. The addr
                #is not correct using mac, maybe windows is right
                
                #here we crack the message format.
                #if(...=='ack'):
                    #if(...==self.server_a):
                        #self.counter_a = self.counter_a+1    
                    #elif(...==self.server_b):
                        #self.counter_b = self.counter_b+1  
                    #elif(...==self.server_c):
                        #self.counter_c = self.counter_c+1  
                    #elif(...==self.server_d): 
                        #self.counter_d = self.counter_d+1                
                #else:          
                
                # Handle Acks
                msg = message.split(" ")
                if msg and msg[0].lower() == 'search':
                    key = ('search', msg[1])
                    if key in heldAcks:
                        try:
                            del heldAcks[key]
                        except KeyError:
                            pass
                    heldAcks[key] = [0, msg[4]]
                elif msg and msg[0].lower() == 'ack':
                    # ackMsg is (ack, cmd) (var) (Yes/No) (self.p)
                    # heldAcks is var : list(count, orginalClient, client1, client2, etc.)
                    key = ('search', msg[1])    # Hard-coded only to work with search
                    cmd = key[0]
                    var = key[1]
                    curList = heldAcks[key]
                    curList[0] = curList[0] + 1
                    if msg[2].lower() != 'no':
                        curList.append(msg[3])
                    if (curList)[0] >= 4:
                        origPort = curList[1]
                        del curList[0:1]
                        searchList = 's' + cmd + ' ' + var + ' ' + ','.join(map(str, curList)) + ' 0 ' + origPort
                        #[Chester] Maybe not broadcast the search result.
                        self._Queue.put(searchList, (self.h, int(origPort)))
                    continue
                
                #[Chester]identify the write model
                #elif msg and (msg[0].lower() == 'insert' or msg[0].lower() == 'update') and (self.model != 1 or self.model !=2):
                    #key includes ('write', key, requester port)
                    #key = ('write', msg[1], msg[4])
                    #if key in heldAcks:
                    #    try:
                    #        del heldAcks[key]
                    #    except KeyError:
                    #        pass
                    #heldAcks[key] = [0, msg[4]] 
                #elif msg and msg[0].lower() == 'write_ack':                                     
                 #   key = ('write', msg[1], msg[4])
                 #   curList = heldAcks[key]
                  #  curList[0] = curList[0]+1               
                    #handle W=1, R=1
                   # if(msg[3] == '3' and curList[0]>=1):
                    #    ackMsg = 'write_ack_model_3'
                     #   self.s_send.sendto(ackMsg, (self.h, int(msg[4])))            
                    #handle W=2, R=2
                    #elif(msg[3] == '4' and curList[0]>=2):
                    #    ackMsg = 'write_ack_model_4'
                    #    self.s_send.sendto(ackMsg, (self.h, int(msg[4]))) 
                    #continue   
                        
                self._Queue.put(message)
                
                #if(self.counter_a == 4):
                    #self.s_send.sendto(ack_msg,(self.h, int(self.server_a)))
                    #self.counter_a=0
                #if(self.counter_b == 4):
                    #self.s_send.sendto(ack_msg,(self.h, int(self.server_b)))
                    #self.counter_b=0
                #if(self.counter_c == 4):
                    #self.s_send.sendto(ack_msg,(self.h, int(self.server_c)))
                    #self.counter_c=0
                #if(self.counter_d == 4):
                    #self.s_send.sendto(ack_msg,(self.h, int(self.server_d)))
                    #self.counter_d=0
                
                
        def send(self):
            sysTime = 0;
            sysTime2= 0;
            while True:
                sysTime2 = round(time.time())
                if(sysTime == sysTime2):
                    pass
                else:
                    sysTime = sysTime2
                if(sysTime % 5 == 0):
                    pass
                    #repair
                
                
                try:
                    message = self._Queue.get()
                except:
                    continue
                if message:
                    for s in self.server_list:
                        self.s_send.sendto(message, (self.h, int(s)))
                

if __name__ == '__main__':   
    if len(sys.argv) <2:
            sys.argv.append(raw_input('Central Server> Please choose the consistency model:\n 1.Linearizability\n 2.Sequential consistency\n 3.Eventual consistency, W=1, R=1\n 4.Eventual consistency, W=2, R=2\n Central Server>'))
    #print sys.argv[1]
    usc = Central_server(sys.argv[1])
    usc.start()
