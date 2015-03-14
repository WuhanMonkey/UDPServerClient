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
heldAcks = {}
 
class UDPServerClient:
        def __init__(self):
            self.s_listen = None    #socket_listen
            self.s_send = None      #socket_send
            self.t_listen = None    #thread_listen
            self.t_send = None      #thread_send
            self.p = None           #port number
            self.Max_delay = None   #max delay
            self.h = None           #HOST
            self.c = None           #central server port number
            self.model = None       #Model of connection
            self.lock = threading.RLock()
            self.server_list = None
       
        def start(self):
            try:
                file = open("config.txt", 'r')
            except IOError:
                print "Error: can\'t find Configuration file"        
            else:
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
 
                self.server_list = file.readline()
                self.server_list = self.server_list.split("=")
                self.server_list = self.server_list[1]
                self.server_list = self.server_list.split(",")
 
                lastPort = file.readline()
                t = lastPort
                lastPort = lastPort.split('=')
                lastPort = lastPort[1].strip()
                if int(lastPort) > 8246:
                    self.p = '8244'
                else:
                    self.p = str(int(lastPort)+1)
                print "The UDP Server had been configured to Port:",self.p
                t = t.replace(t, 'PORT= ' + self.p)
                x=open("config.txt", 'r')
                t1 = x.readline()
                t2 = x.readline()
                t3 = x.readline()
                t4 = x.readline()
                x.close()
                t = t1 + t2 + t3 + t4 + t
                #print t
                open('config.txt', 'w').close()
                s=open("config.txt", 'a')
                s.write(t)
                s.close()
 
                self.s_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_listen.bind(('',int(self.p)))                
                self.t_listen = threading.Thread(target=self.listen)
                self.t_listen.start()
                self.t_send = threading.Thread(target=self.send)
                self.t_send.start()
        def listen(self):
            while 1:
                    #receive msg
                msg,addr = self.s_listen.recvfrom(1024)
               
                msg = msg.split()
                msg_size = len(msg)
                recv_port = msg[msg_size-1]
 
                mt = msg
                if mt[0].lower() == 'admin_model':
                    self.model = int(mt[1])
                    self.c = mt[2]
                    ackMsg = 'ack ' + self.p
                    self.s_send.sendto(ackMsg, (self.h, int(self.c)))
                    print '\nReceived admin_model and sent ack. Model is %s' % self.model
                    continue
 
                #msg[msg_size-1] = ':'
                #msg[msg_size-2] = 'from'
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
            try:
                for k in msgQueue:
                    if msgQueue[k].empty() == True:
                        continue
                    q = msgQueue[k].queue[0]
                    if q[0] + q[3] <= time.time():
                        job = msgQueue[k].get()
                        processQueue.put((job[3], job[0], job[2], job[4]))  #randDelay, sysTime, msg, recv_port
            except RuntimeError:
                pass
 
            if processQueue.qsize() > 0:
                while processQueue.empty() == False:
                    msgTuple = processQueue.get()
                    # delay, sysTime, (cmd, var, val), recv_port
                    msg = msgTuple[2]
                    recv_port = msgTuple[3]
                    delay = msgTuple[0]
                    sysTime = msgTuple[1]
 
                    msgM = msg.split(" ")
                    #if msgM[0].lower() == 'search':
                    #    continue
                    #if not(msgM[0].lower() != 'aget' or msgM[0].lower() != 'ssearch' or msgM[0].lower() != 'aupdate' or msgM[0].lower() != 'ainsert'):
                    print '\nReceived %s from port %s, Job delay is %s, Sys Time is %s' % (msg, recv_port, delay, sysTime) #self.Max_delay
 
                    #print '0: %s, 1: %s, 2: %s' % (msgM[0], msgM[1], msgM[2])
 
                    # (cmd, var, val)
                    cmd = msgM[0].lower()
                    var = msgM[1]
                    if cmd == 'get':
                        model = msgM[2]
                    else:
                        val = msgM[2]
                    if cmd == 'insert' or cmd == 'update':
                        model = msgM[3]
                    #[Chester] Needs to ack with the requester port number to the central server for cmd insert and update: eg. ack_write
                    if cmd == 'insert' or cmd == 'update':
                        if not (cmd == 'update' and not var in data):
                            if var in data and (data[var])[1] > sysTime:
                                print 'Old value had larger timestamp, no change done'
                            else:
                                data[var] = (val, sysTime)
                                print 'Value %s written to variable %s' % (data[var], var)
                        else:
                            print 'Variable %s does not exist, so no update done' % var
 
                        if model > 2:
                            if cmd == 'insert':
                                insert_ack = 'ainsert' + ' ' + var + ' '  + val + ' ' + model + ' ' + self.p
                                self.s_send.sendto(insert_ack, (self.h, int(recv_port)))
                            if cmd == 'update':
                                update_ack = 'aupdate' + ' ' + var + ' ' + val + ' ' + model + ' ' + self.p
                                self.s_send.sendto(update_ack, (self.h, int(recv_port)))
                    elif cmd == 'ainsert' or cmd == 'aupdate':
                        if cmd == 'ainsert':
                            key = ('insert', msgM[1], msgM[2], msgM[3])
                        else:
                            key = ('update', msgM[1], msgM[2], msgM[3])
                        if key in heldAcks:
                            model = msgM[3]
                            print cmd + model
                            curList = heldAcks[key]
                            curList[0] = curList[0] + 1
                            print curList[0]
                            if (msgM[3] == '3' and curList[0] >= 1) or (msgM[3] =='4' and curList[0] >=2):
                                print 'The %s %s %s reqest has been done on model %s, the ack_number is %s' %(key[0], msgM[1], msgM[2],msgM[3],curList[0])
                                if key in heldAcks:
                                    try:
                                        del heldAcks[key]
                                    except KeyError:
                                        pass
                    elif cmd == 'get':
                        if self.model == 1:
                            if var in data:
                                print 'Variable %s has value %s' % (var, data[var][0])
                            else:
                                print 'Var %s doesn\'t exist in local replica' % var
                        if model > 2:
                            if var in data:
                                getAck = 'aget ' + var + ' ' + model + ' ' + str(data[var][0]) + ' ' + str(data[var][1]) + ' ' + self.p
                            else:
                                getAck = 'aget ' + var + ' ' + model + ' none 0 ' + self.p
                        self.s_send.sendto(getAck, (self.h, int(recv_port)))
                    elif cmd == 'aget':
                        # (get) (var) (model) (val) (timestamp) 
                        key = ('get', msgM[1], msgM[2])
                        if key in heldAcks:
                            model = msgM[2]
                            #print cmd + ' ' + model
                            curList = heldAcks[key]
                            curList[0] = curList[0] + 1
                            #print curList
                            #print msgM[4]
                            if not(msgM[4] == curList[2]) or not(msgM[3] == curList[1]):
                                curList[3] = curList[3] + 1
                            if msgM[4] > curList[2]:
                                curList[2] = msgM[4] # Timestamp
                                curList[1] = msgM[3] # Value

                            if (msgM[2] == '3' and curList[0] == 1) or (msgM[2] == '4' and curList[0] == 2):
                                print 'The %s %s request has been done on model %s, the ack_number is %s' %(key[0], msgM[1], msgM[2],curList[0])
                                if var in data:
                                    print 'Variable %s has value %s' % (var, data[var][0])
                                else:
                                    print 'Var %s doesn\'t exist in local replica' % var
                            elif curList[0] >= 4:
                                if curList[3] > 0:
                                    print 'Inconsistency in %s detected, sending repair messages' % (msgM[1])
                                    repair = 'insert ' + msgM[1] + ' ' + curList[1] + ' ' + model + ' ' + self.p
                                    for s in self.server_list:
                                        self.s_send.sendto(repair, (self.h, int(s)))
                                else:
                                    print 'No inconsistency detected'
                                if key in heldAcks:
                                    try:
                                        del heldAcks[key]
                                    except KeyError:
                                        pass
                    elif cmd == 'delete':
                        if var in data:
                            with self.lock:
                                try:
                                    del data[var]
                                except KeyError:
                                    pass
                    elif cmd == 'show-all' and recv_port == self.p:
                        with self.lock:
                            print '\n'
                            for k,v in data.iteritems():
                                print 'Var %s has value %s' % (k,v[0])
                            if not data:
                                print 'Nothing stored on the local replica currently'
                    elif cmd == 'search':
                        if var in data:
                            ackMsg = 'ack ' + var + ' Yes ' + self.p
                        else:
                            ackMsg = 'ack ' + var + ' No ' + self.p
                        self.s_send.sendto(ackMsg, (self.h, int(self.c)))
                    elif cmd == 'ssearch':
                        print 'Servers %s have var %s' % (val, var)
                    print 'UDP Server Client> Enter input file:'
           
            if exitFlag == False:
                Timer(0.5, self.checkAck, ()).start()
                time.sleep(0.55)
             
        def send(self):
            global exitFlag
            input_flag = True
            while(True):
                while input_flag:
                    msg = raw_input('UDP Server Client> Enter input file:')
                    try:
                        input_file = open(msg,'r')
                    except IOError:
                        print "UDP Server Client> Error: can\'t find the input file"
                    else:
                        input_flag = False
            #print "UDP Server Client> Input file load success!"
           
                while True:
                    msg = input_file.readline().rstrip()
                    if msg == '':
                        input_flag =True
                        break
               
                    msg_sp = msg.split(" ")
 
                    if (msg_sp[0].lower() == 'get' and self.model == 2):
                        print 'Received get %s in Model %s mode' % (msg_sp[1],self.model)
                        try:
                            print 'Variable %s has value %s, SEQ' % (msg_sp[1], data[msg_sp[1]])
                        except KeyError:
                            print "Var %s doesn\'t exist in local replica" % msg_sp[1]
                    elif (msg_sp[0].lower() == 'delay'):
                        time.sleep(float(msg_sp[1]))
                        continue
                    elif((msg_sp[0].lower() =='insert' or msg_sp[0].lower() =='update') and (self.model == 3 or self.model == 4)):
                        #send to other server and wait for write_ack.
                        if msg_sp[0].lower() == 'insert':
                            key = ('insert', msg_sp[1], msg_sp[2], msg_sp[3])
                        else:
                            key = ('update', msg_sp[1], msg_sp[2], msg_sp[3])
                        if key in heldAcks:
                            try:
                                del heldAcks[key]
                            except KeyError:
                                pass
                        heldAcks[key] = [0, self.p]
                        msg = msg + ' ' + str(self.p)
                        for s in self.server_list:
                            self.s_send.sendto(msg, (self.h, int(s)))
                        continue
                    elif(msg_sp[0].lower() == 'get' and (self.model == 3 or self.model == 4)):
                        key = ('get', msg_sp[1], msg_sp[2])
                        if key in heldAcks:
                            try:
                                del heldAcks[key]
                            except KeyError:
                                pass
                        heldAcks[key] = [0, msg_sp[1], 0, -1, self.p]
                        msg = msg + ' '+ str(self.p)
                        for s in self.server_list:
                            self.s_send.sendto(msg, (self.h, int(s)))                
                        continue
               
               
                    else:
                    #[Chester] Needs to adapt to the model
                        if (msg_sp[0].lower() == 'get' and self.model == 1):
                            msg = msg + ' 0 ' + str(self.p)
                    #[Chester] get cmd adapt to model 3 and 4.    
 
                        elif (msg_sp[0].lower() == 'delete' or msg_sp[0].lower() == 'search'):
                            msg = msg + ' 0 0 ' + str(self.p)
                    #[Chester] Needs to adapt to the model, handle on receive side.  
                        elif((msg_sp[0].lower() =='insert' or msg_sp[0].lower() =='update') and (self.model == 1 or self.model == 2)):
                            msg = msg + ' ' + str(self.p)
                                 
                        elif (msg.lower() == 'show-all'):
                        #print '[to-do]show all local replica'
                        # @TODO[Kelsey] What do you mean 'show all local replica'?
                            msg = msg + ' 0 0 0 ' + str(self.p)
                        elif msg.lower() == 'search':
                            msg = msg + ' 0 ' + str(self.p)
                        try:
                            self.s_send.sendto(msg, (self.h, int(self.c)))
                        except socket.error, msg:
                            print'UDP Server Client> Error Code : ' + str(msg[0]) + 'Message' +msg[1]
                    #elif(msg[0]== 'Stop' or msg[0] == 'stop'):
                    #    print "UDP Server Client will now exit"
                    #    self.s_listen.close()
                    #    self.s_send.close()
                    #    print "Socket closed"
                    #    exitFlag = True
                    #    break
                    #    sys.exit(0)
               
       
usc = UDPServerClient()
sys.stdout.write("UDP Server Client> ")
usc.start()