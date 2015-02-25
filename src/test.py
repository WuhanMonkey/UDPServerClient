import socket
import threading
import sys

msg=raw_input('Enter message to send:')
msg = msg.split()
msg_size = len(msg)
print msg_size
    
print msg[msg_size-1]

for i in range (0, msg_size-1):
    msg[i] = msg[i+1]
msg = ' '.join(msg)

print msg