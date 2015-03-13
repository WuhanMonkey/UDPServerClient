import socket
import threading
import sys
import time


first_flag = True
second_flag = True
while first_flag:
    while second_flag:
        msg = raw_input("input:")
        if(msg == '1'):
            second_flag = False
    while(True):
        msg = raw_input("input2:")
        if(msg=='1'):
            second_flag = True
            break