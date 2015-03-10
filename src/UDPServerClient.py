#cs425 mp1
import socket
import threading
import sys

import Queue, random, time
from threading import Timer
from Queue import PriorityQueue
import fileinput

# Hashtable of Priority Queues, append if new host found
msgQueue = {}
counter = 0
exitFlag = False
data = {}

class UDPServerClient:
        def __init__(self):
            self.s_listen=None #socket_listen
            self.s_send=None#socket_send
            self.t_listen=None #thread_listen
            self.t_send=None #thread_send
            self.p=None #port number
            self.Max_delay=None #max delay
            self.h=None #HOST
            self.c=None #central server port number
            self.model=None #Model of connection
        
        def start(self):
            try:
                file=open("config.txt", 'r')
            except IOError:
                print "Error: can\'t find Configuration file"         
            else:
                # @TODO[Kelsey] Presumes config file in format "CENTRAL= xxxx, correct for
                # when central server is written
                #central_server = file.readline()
                #central_server = central_server.split("=")
                #central_server = central_server[1].strip()
                #self.c = central_server
                #print "UDP Server Client> The UDP Server has been configured to central server:", central_server

                t = file.readline()
                
                max_delay = file.readline()    
                max_delay = max_delay.split("=")
                max_delay = max_delay[1].strip()
                self.Max_delay = max_delay
                print "UDP Server Client> The UDP Server had been configured to Max delay:",max_delay
                host = file.readline()    
                host = host.split("=")
                host = host[1].strip()
                self.h = host
                print "UDP Server Client> The UDP Server had been configured to host:",host

                t = file.readline()

                #port = file.readline()    
                #port = port.split("=")
                #port = port[1].strip()
                #port = port.split(",")

                lastPort = file.readline()
                t = lastPort
                lastPort = lastPort.split('=')
                lastPort = lastPort[1].strip()
                if int(lastPort) > 9013:
                    self.p = '9011'
                else:
                    self.p = str(int(lastPort)+1)
                print "The UDP Server had been configured to Port:",self.p
                t = t.replace(t, 'PORT= ' + self.p)
                s=open("config.txt", 'w')
                t = 'PORT_NUMBER= 9029\nMAX_DELAY= 5\nHOST= localhost\nSERVER_LIST=9011,9012,9013,9014\n' + t
                s.write(t)
                s.close()

                self.s_listen=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_send=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_listen.bind(('',int(self.p)))                
                self.t_listen=threading.Thread(target=self.listen)
                self.t_listen.start()
                self.t_send=threading.Thread(target=self.send)
                self.t_send.start()
        def listen(self):
            while 1:
                    #receive msg
                msg,addr=self.s_listen.recvfrom(1024)
                
                msg = msg.split()
                msg_size = len(msg)
                recv_port = msg[msg_size-1]

                #print msg
                mt = msg #msg.split(" ")
                if mt[0].lower() == 'admin_model':
                    self.model = mt[1]
                    print self.model
                    self.c = mt[2]
                    ackMsg = 'ack ' + self.p
                    self.s_send.sendto(ackMsg, (self.h, int(self.c)))

                msg[msg_size-1]=':'
                msg[msg_size-2]='from'
                msg = ' '.join(msg)

                if not msg:
                    break

                global msgQueue # Hashtable of Priority Queues
                global counter
                global exitFlag
                global data     # Hashtable for Data w/ key = var

                # Do not handle until popped off of priority queue
                if msgQueue.get(recv_port) == None:
                    q = PriorityQueue()
                    msgQueue[recv_port] = q
                else:
                    q = msgQueue[recv_port]

                counter+=1
                jobDelay = random.randint(0, int(self.Max_delay))
                q.put((time.time(), counter, msg, jobDelay, recv_port))
                # Checks the queue perodically
                Timer(0.5, self.checkAck, ()).start()

        def checkAck(self):
            processQueue = PriorityQueue();
            for k in msgQueue:
                if msgQueue[k].empty() == True:
                    continue
                q = msgQueue[k]
                if (msgQueue[k].queue[0])[0] + (msgQueue[k].queue[0])[3] <= time.time():
                    job = msgQueue[k].get()
                    processQueue.put((job[3], job[0], job[2], job[4]))  #randDelay, sysTime, msg, recv_port

            if processQueue.qsize() > 0:
                while processQueue.empty() == False:
                    msgTuple = processQueue.get()
                    # delay, sysTime, (cmd, var, val), recv_port
                    msg = msgTuple[2]
                    recv_port = msgTuple[3]
                    delay = msgTuple[0]
                    sysTime = msgTuple[1]
                    print 'Received %s port %s, Job delay is %s, Sys Time is %s' % (msg, recv_port, delay, sysTime) #self.Max_delay

                    msgM = msg.split(" ")
                    #print '0: %s, 1: %s, 2: %s' % (msgM[0], msgM[1], msgM[2])

                    # (cmd, var, val)
                    cmd = msgM[0].lower()
                    var = msgM[1]
                    val = msgM[2]

                    #if cmd != 'ack' and cmd != 'Ack':
                    if cmd == 'insert' or cmd == 'update':
                        if not (cmd == 'update' and not var in data):
                            data[var] = val
                            print 'Value %s written to variable %s' % (data[var], var)
                        else:
                            print 'Variable %s does not exist, so no update done' % var

                    if (cmd == 'get') and recv_port == self.p:
                            # @TODO[Kelsey] Check if sending back to central server is needed
                            if var in data:
                                print 'Variable %s has value %s' % (var, data[var])
                            else:
                                print "Var doesn't exist in local replica"

                        #ackMsg = 'ack 0 ' + self.c
                        #self.s_send.sendto(ackMsg, (self.h, int(self.c)))
                    if cmd == 'delete':
                        if var in data:
                            try:
                                del data[var]
                            except KeyError:
                                pass
                print 'UDP Server Client> Enter message to send:'
            
            if exitFlag == False:
                Timer(0.5, self.checkAck, ()).start()
                time.sleep(0.55)
              
        def send(self):
            global exitFlag

            while True:
                msg=raw_input('UDP Server Client> Enter message to send:\n')
                if msg.lower() != 'send':
                    msg = msg + ' 0 0 0'
                msg = msg.split()
                if msg[1].lower() == 'get' and self.model == 2:
                    print 'Variable %s has value %s, SEQ' % (msg[2], data[msg[2]])
                else:
                    if(msg[0].lower() == 'send'):
                        msg_size = len(msg)
                        send_port = msg[msg_size-1]            
                        for i in range (0, msg_size-1):
                            msg[i] = msg[i+1]
                        msg[msg_size-1] = self.p
                        msg = ' '.join(msg)
                
                    # @TODO[Kelsey] Error check without crashing program
                        try:
                         self.s_send.sendto(msg, (self.h, int(send_port)))
                        except socket.error, msg:
                          print'Error Code : ' + str(msg[0]) + 'Message' +msg[1]
                    elif(msg[0].lower() == 'stop'):
                        #print "UDP Server Client will now exit"
                        self.s_listen.close()
                        self.s_send.close()
                        print "Socket closed"
                        exitFlag = True
                        break
                        #sys.exit(0)
                    elif(msg[0].lower() == 'help'):
                        print "Use the format (Send/Stop) (Message Contents) (Port Number)"
                        print "Message Contents is in format (Command) (Variable) (Value)\n"
                    else: 
                        print "Wrong command, Enter again\n UDP Server Client> Enter message to send:\n"
                
        
usc = UDPServerClient()
sys.stdout.write("UDP Server Client> ")
usc.start()
