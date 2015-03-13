import socket
import threading
import sys
import time
sysTime = 0;
sysTime2= 0;
while True:
    sysTime2 = round(time.time())
    if(sysTime == sysTime2):
        pass
    else:
        sysTime = sysTime2
        if(sysTime % 5 == 0):
            print time.time()