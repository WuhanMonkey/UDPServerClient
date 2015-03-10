import socket
import threading
import sys
p = 123
msg = 'get 3'
msg = msg+' '+str(p)
print msg

msg = msg.split()
msg_size = len(msg)
recv_port = msg[msg_size-1]
print recv_port