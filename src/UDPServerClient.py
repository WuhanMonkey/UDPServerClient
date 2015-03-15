#cs425 mp1
import socket
import threading
import sys
 
import Queue, random, time
from threading import Timer
from Queue import PriorityQueue
import fileinput
 
msgQueue = {}   # Channel Hashtable of Priority Queues, append if new host found
counter = 0
exitFlag = False
data = {}       # Local Replica Data Storage, Hashtable ..... format is variable -> (value, timestamp)
heldAcks = {}   # Acknowledge Message Hashtable, stores necessary ack data
 
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
                # Read through 'config.txt' for setup details
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

                # Replace the Port Number so next Client has a different Port Number
                t = t.replace(t, 'PORT= ' + self.p)
                x=open("config.txt", 'r')
                t1 = x.readline()
                t2 = x.readline()
                t3 = x.readline()
                t4 = x.readline()
                x.close()
                t = t1 + t2 + t3 + t4 + t
                open('config.txt', 'w').close()
                s=open("config.txt", 'a')
                s.write(t)
                s.close()
 
                # Start threads for listening for and sending messages
                self.s_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.s_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.s_listen.bind(('',int(self.p)))                
                self.t_listen = threading.Thread(target=self.listen)
                self.t_listen.start()
                self.t_send = threading.Thread(target=self.send)
                self.t_send.start()

        # The Listening Thread pops Messages off of the task queue and handles them sequentially
        def listen(self):
            global msgQueue
            global counter
            global exitFlag
            global data

            while 1:
                # Receive Message
                msg,addr = self.s_listen.recvfrom(1024)
               
                msg = msg.split()
                msg_size = len(msg)
                recv_port = msg[msg_size-1]
 
                # Handle the Set-up message from the Central Server, which details the default Model
                # for message handling as well as the Central Server's port number
                mt = msg
                if mt[0].lower() == 'admin_model':
                    self.model = int(mt[1])
                    self.c = mt[2]
                    ackMsg = 'ack ' + self.p
                    self.s_send.sendto(ackMsg, (self.h, int(self.c)))
                    print '\nReceived admin_model and sent ack. Model is %s' % self.model
                    continue
 
                msg = ' '.join(msg)
                if not msg:
                    break
 
                # Do not handle until popped off of priority queue
                if msgQueue.get(recv_port) == None:
                    q = PriorityQueue()
                    msgQueue[recv_port] = q
                else:
                    q = msgQueue[recv_port]
 
                counter+=1
                jobDelay = random.randint(0, int(self.Max_delay))
                q.put((time.time(), counter, msg, jobDelay, recv_port))
                # Uses a scheduler to schedule checking of the queue every 0.5 seconds
                Timer(0.5, self.checkAck, ()).start()
 
        def checkAck(self):
            processQueue = PriorityQueue();
            try:
                # Iterate sequentially over all the Channel Queues, pop off a channel queue if the 
                # current system time is smaller than the Channel Queue's top msg's time + delay
                for k in msgQueue:
                    if msgQueue[k].empty() == True:
                        continue
                    q = msgQueue[k].queue[0]
                    if q[0] + q[3] <= time.time():
                        job = msgQueue[k].get()
                        processQueue.put((job[3], job[0], job[2], job[4]))  # Format is randDelay, sysTime, msg, recv_port
            except RuntimeError:
                pass
 
            # Sequentially process all the messages in the To-Be-Process Queue
            if processQueue.qsize() > 0:
                while processQueue.empty() == False:
                    msgTuple = processQueue.get()
                    # msgTuple format is delay, sysTime, (cmd, var, val), recv_port
                    msg = msgTuple[2]
                    recv_port = msgTuple[3]
                    delay = msgTuple[0]
                    sysTime = msgTuple[1]
 
                    msgM = msg.split(" ")
                    print '\nReceived %s from port %s, Job delay is %s, Sys Time is %s' % (msg, recv_port, delay, sysTime)
 
                    # Identify which portions of msgM are the commands, variables, values, and model number
                    # This is necessary since the length of msgM differs depends on the command
                    cmd = msgM[0].lower()
                    var = msgM[1]
                    if cmd == 'get':
                        model = msgM[2]
                    else:
                        val = msgM[2]
                    if cmd == 'insert' or cmd == 'update':
                        model = msgM[3]
                    
                    if cmd == 'insert' or cmd == 'update':
                        # Only add the value if it is a newer command than the one already in the local replica
                        # Also only updates if the variable exists in the local replica
                        if not (cmd == 'update' and not var in data):
                            if var in data and (data[var])[1] > sysTime:
                                print 'Old value had larger timestamp, no change done'
                            else:
                                data[var] = (val, sysTime)
                                print 'Value %s written to variable %s' % (data[var], var)
                        else:
                            print 'Variable %s does not exist, so no update done' % var
 
                        # If the message is using Eventual Consistency, we'll send the message directly to each other
                        # instead of passing to the central server to be broadcast
                        if model > 2:
                            if cmd == 'insert':
                                insert_ack = 'ainsert' + ' ' + var + ' '  + val + ' ' + model + ' ' + self.p
                                self.s_send.sendto(insert_ack, (self.h, int(recv_port)))
                            if cmd == 'update':
                                update_ack = 'aupdate' + ' ' + var + ' ' + val + ' ' + model + ' ' + self.p
                                self.s_send.sendto(update_ack, (self.h, int(recv_port)))

                    elif cmd == 'ainsert' or cmd == 'aupdate':
                        # Handle the Acknowledgement messages for the Eventual Consistency versions of Insert and Update
                        if cmd == 'ainsert':
                            key = ('insert', msgM[1], msgM[2], msgM[3])
                        else:
                            key = ('update', msgM[1], msgM[2], msgM[3])

                        if key in heldAcks:
                            model = msgM[3]
                            curList = heldAcks[key]
                            curList[0] = curList[0] + 1     # Increment the counter for number of Acks recieved thus far

                            # Clean-up with Printing response and Deleting the Acks after the required amount of Acks have been received
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
                        # Handle eventual consistency by sending Acks messages to the requesting Port with the variable, value, timestamp, and model number
                        # This information will be used to locate the proper entry in the heldAcks hashtable
                        if model > 2:
                            if var in data:
                                getAck = 'aget ' + var + ' ' + model + ' ' + str(data[var][0]) + ' ' + str(data[var][1]) + ' ' + self.p
                            else:
                                getAck = 'aget ' + var + ' ' + model + ' none 0 ' + self.p
                        self.s_send.sendto(getAck, (self.h, int(recv_port)))

                    elif cmd == 'aget':
                        # Aget message format is (get) (var) (model) (val) (timestamp) 
                        key = ('get', msgM[1], msgM[2])
                        if key in heldAcks:
                            model = msgM[2]
                            curList = heldAcks[key]
                            curList[0] = curList[0] + 1     # Increment the counter for number of Acks recieved thus far

                            # We want to increment the counter if the timestamp or value for the is different from the one in local storage
                            # Like a flag this will tell us we need to do an inconsistency repair
                            if not(msgM[4] == curList[2]) or not(msgM[3] == curList[1]):
                                curList[3] = curList[3] + 1

                            # If the message's timestamp is larger than the one in local storage, we should replace it
                            if msgM[4] > curList[2]:
                                curList[2] = msgM[4] # Timestamp
                                curList[1] = msgM[3] # Value

                            # Handle Acks
                            if (msgM[2] == '3' and curList[0] == 1) or (msgM[2] == '4' and curList[0] == 2):
                                print 'The %s %s request has been done on model %s, the ack_number is %s' %(key[0], msgM[1], msgM[2],curList[0])
                                if var in data:
                                    print 'Variable %s has value %s' % (var, data[var][0])
                                else:
                                    print 'Var %s doesn\'t exist in local replica' % var
                            # Handle Inconsistency Repair by sending an Insert to all the nodes with the correct value
                            elif curList[0] >= 4:
                                if curList[3] > 0:
                                    print 'Inconsistency in %s detected, sending repair messages' % (msgM[1])
                                    repair = 'insert ' + msgM[1] + ' ' + curList[1] + ' ' + model + ' ' + self.p
                                    for s in self.server_list:
                                        self.s_send.sendto(repair, (self.h, int(s)))
                                else:
                                    print 'No inconsistency detected'

                                # Cleanup the heldAcks hashtable by deleting the key after all the Acks have been received
                                if key in heldAcks:
                                    try:
                                        del heldAcks[key]
                                    except KeyError:
                                        pass

                    elif cmd == 'delete':
                        if var in data:
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
                        # Response to a search call by sending an ack to the central server with whether or not the local replica has the variable
                        if var in data:
                            ackMsg = 'ack ' + var + ' Yes ' + self.p
                        else:
                            ackMsg = 'ack ' + var + ' No ' + self.p
                        self.s_send.sendto(ackMsg, (self.h, int(self.c)))

                    elif cmd == 'ssearch':
                        # Print the result, i.e. which servers have the variable
                        print 'Servers %s have var %s' % (val, var)

                    print 'UDP Server Client> Enter input file:'
           
            # If we're not exiting, scheduling the next check of the message queue
            if exitFlag == False:
                Timer(0.5, self.checkAck, ()).start()
                time.sleep(0.55)

        # This thread is for handling the commands entered via an inputfile.
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
           
                while True:
                    msg = input_file.readline().rstrip()
                    if msg == '':
                        input_flag =True
                        break
               
                    msg_sp = msg.split(" ")
 
                    if (msg_sp[0].lower() == 'get' and self.model == 2):
                        # For Get in Sequential Consistency, we don't broadcast the request at all and simply check and return immediately
                        print 'Received get %s in Model %s mode' % (msg_sp[1],self.model)
                        try:
                            print 'Variable %s has value %s, SEQ' % (msg_sp[1], data[msg_sp[1]])
                        except KeyError:
                            print "Var %s doesn\'t exist in local replica" % msg_sp[1]

                    elif (msg_sp[0].lower() == 'delay'):
                        time.sleep(float(msg_sp[1]))
                        continue


                    elif((msg_sp[0].lower() =='insert' or msg_sp[0].lower() =='update') and (self.model == 3 or self.model == 4)):
                        # Insert and Update in Eventual Consistency.... we create the key and entry for the heldAcks hashtable now
                        # Send to other servers and wait for write ack.
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
                        # Get in Eventual Consistency.... we create the key and entry for the heldAcks hashtable now
                        key = ('get', msg_sp[1], msg_sp[2])
                        if key in heldAcks:
                            try:
                                del heldAcks[key]
                            except KeyError:
                                pass

                        # Format is (no. Acks received) (value) (timestamp) (counter for whether or not we need to change) (requester port number)
                        heldAcks[key] = [0, msg_sp[1], 0, -1, self.p]
                        msg = msg + ' '+ str(self.p)

                        for s in self.server_list:
                            self.s_send.sendto(msg, (self.h, int(s)))                
                        continue

                    # Handle the commands without Eventual Consistency such as Delete, Show-all, Search
                    # as well as the Model 1 versions of Get and Model 1 and 2 versions of Insert, and Update
                    else:
                        if (msg_sp[0].lower() == 'get' and self.model == 1):
                            msg = msg + ' 0 ' + str(self.p)
 
                        elif (msg_sp[0].lower() == 'delete' or msg_sp[0].lower() == 'search'):
                            msg = msg + ' 0 0 ' + str(self.p)
                        elif((msg_sp[0].lower() =='insert' or msg_sp[0].lower() =='update') and (self.model == 1 or self.model == 2)):
                            msg = msg + ' ' + str(self.p)
                        elif (msg.lower() == 'show-all'):
                            msg = msg + ' 0 0 0 ' + str(self.p)
                        elif msg.lower() == 'search':
                            msg = msg + ' 0 ' + str(self.p)

                        try:
                            self.s_send.sendto(msg, (self.h, int(self.c)))
                        except socket.error, msg:
                            print'UDP Server Client> Error Code : ' + str(msg[0]) + 'Message' +msg[1]
       
usc = UDPServerClient()
sys.stdout.write("UDP Server Client> ")
usc.start()