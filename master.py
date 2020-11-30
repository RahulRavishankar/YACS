import json
import socket
import time
import sys
import random
import numpy

args = sys.argv
path = args[1]
algo = args[2]



if __name__ == '__main__':
	if(len(sys.argv)!=2):
		print("Usage: python master.py <path to config file> <name of algorithm: RR,LL,RANDOM")
		exit()
		if(algo == "RR"):
			handle_roundrobin()
		elif(algo == "RANDOM"):
			handle_random()
		elif(algo == "LL"):
			handle_LL()
		else:
			print("INVALID ALGORITHM! ENTER RR,LL or RANDOM")
			exit()
