import json
import socket
import time
import sys
import random
import numpy

args = sys.argv
port = args[1]
worker_id = args[2]



if __name__ == '__main__':
	if(len(sys.argv)!=2):
		print("Usage: python worker.py <port> <worker_id>")
		exit()
