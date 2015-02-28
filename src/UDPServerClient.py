#cs425 mp1
import socket
import threading
import sys

import Queue, random, time
from threading import Timer
from Queue import PriorityQueue

# Buffer of Priority Queues, append if new host found
msgQueue = {}
counter = 0
exitFlag = False

class UDPServerClient:
        def __init__(self):
            self.s_listen=None #socket_listen
            self.s_send=None#socket_send
            self.t_listen=None #thread_listen
            self.t_send=None #thread_send
            self.p=None #port number
            self.Max_delay=None #max delay
            self.h=None #HOST
        
        def start(self):
            try:
                file=open("config.txt", 'r')
            except IOError:
                print "Error: can\'t find Configuration file"         
            else:
                port = file.readline()    
                port = port.split("=")
                port = port[1].strip()
                self.p = port
                print "The UDP Server had been configured to Port:",port
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
                msg[msg_size-1]=':'
                msg[msg_size-2]='from'
                msg = ' '.join(msg)

                if not msg:
                    break
               
                # Buffer of Priority Queues, append if new host found
                global msgQueue
                global counter
                global exitFlag

                # Do not handle until popped off of priority queue
                if msgQueue.get(recv_port) == None:
                    q = PriorityQueue()
                    msgQueue[recv_port] = q
                else:
                    q = msgQueue[recv_port]

                #print 'Processing Message, %s\n' % (msgQueue[recv_port].queue)

                ## @TODO[Kelsey] Check if python's PriorityQueue() handles sorting by default
                counter+=1
                q.put((time.time() + random.randrange(0, int(self.Max_delay)), counter, msg, self.Max_delay, recv_port))
                # Checks the queue perodically
                Timer(0.5, self.checkAck, ()).start()

        def checkAck(self):
            ## @TODO[Kelsey] currently vulnerable to overflow of messages, fix via multi message handling
            processQueue = PriorityQueue();
            for k in msgQueue:
                if msgQueue[k].empty() == True:
                    continue
                q = msgQueue[k]
                if (msgQueue[k].queue[0])[0] <= time.time():
                    processQueue.put(msgQueue[k].get())

            if processQueue.qsize() > 0:
                while processQueue.empty() == False:
                    msgTuple = processQueue.get()
                    msg = msgTuple[2]
                    recv_port = msgTuple[4]
                    delay = msgTuple[3]
                    print 'Received %s port %s, Max delay is %s' % (msg, recv_port, delay) #self.Max_delay
                print 'UDP Server Client> Enter message to send:'
            
            if exitFlag == False:
                Timer(0.5, self.checkAck, ()).start()
                time.sleep(0.55)
              
        def send(self):
            global exitFlag

            while True:
                msg=raw_input('UDP Server Client> Enter message to send:\n')
                msg = msg.split()
                if(msg[0]== 'Send'):
                    msg_size = len(msg)
                    send_port = msg[msg_size-1]            
                    for i in range (0, msg_size-1):
                        msg[i] = msg[i+1]
                    msg[msg_size-1] = self.p
                    msg = ' '.join(msg)
                
                    try:
                        self.s_send.sendto(msg, (self.h, int(send_port)))
                    except socket.error, msg:
                        print'Error Code : ' + str(msg[0]) + 'Message' +msg[1]
                elif(msg[0]== 'Stop'):
                #    print "UDP Server Client will now exit"
                    self.s_listen.close()
                    self.s_send.close()
                    print "Socket closed"
                    exitFlag = True
                    break
                #    sys.exit(0)
                elif(msg[0] == 'Help'):
                    print "Use the format (Send/Stop) (Message Contents) (Port Number)\n"
                else: 
                    print "Wrong command, Enter again\n UDP Server Client> Enter message to send:\n"
                
        
usc = UDPServerClient()
sys.stdout.write("UDP Server Client> ")
usc.start()
