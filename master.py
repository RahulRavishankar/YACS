import json
import socket
import time
import sys
import random
import numpy







def handle_roundrobin():
   pass
def handle_random():
   pass
def handle_LL():
   pass


    
  

if __name__ == '__main__':
        if(len(sys.argv)!=3):
                print("Usage: python master.py <path to config file> <name of algorithm: RR,LL,RANDOM>")    
                exit()        
        args = sys.argv
        path = args[1]
        algo = args[2]
        
        if (algo != "RR") and (algo != "RANDOM") and (algo != "LL"):
           print("INVALID ALGORITHM! ENTER RR,LL or RANDOM")
           exit()



        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
              s.bind(('localhost',5000))
              s.listen(1)
              
              while True:
                 c,addr = s.accept()
                 
                 with c:
                     data = c.recv(1024)
                     if not data:
                        break
                     print(data.decode())
                 
                    
                  
               
                       
        if algo == "RR":
                handle_roundrobin()
        elif algo == "RANDOM":
                handle_random()
        elif algo == "LL":
                handle_LL()
        
                
        
