#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import json
from functools import wraps
import os
import nltk
import random
# import json
from subprocess32 import STDOUT, check_output as qx
from collections import Counter
import sqlite3
import nltk
import time
from joblib import Parallel, delayed
import multiprocessing
import csv
import urllib3

youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&channelId="

def apiActivityGet(uid,idType):
	
	# build api url
	if idType == 'name':
		url = youtubeGetChannelIdUrl
		url += uid

		#response = json.load( urllib2.urlopen( url ) )
		#print type(urllib2.urlopen( url ))
		#print "\n***Processing " + track_file + "\n"
		http = urllib3.PoolManager()
		r = http.request('GET',url)
		#print type(r)
		response = json.loads(r.data)
		#print response
		#response = json.load( urllib2.urlopen( url ) )
		#print response
		channelId = None
		try:
			#parse response body
			channelId = response['items'][0]['snippet']['channelId']
		except:
			pass

	if idType == 'channel':
		channelId = uid

	url = youtubeGetActivityUrl
	url += channelId
	# print url
	#print "\n***Processing " + track_file + "\n"
	http = urllib3.PoolManager()
	r = http.request('GET',url)
	response = json.loads(r.data)

	#response = json.load( urllib2.urlopen( url ) )
	#print response
	videoIds = []
	publicationTimes = []
	# print response
	try:
		#parse response body
		for activity in response['items']:
			# print activity
			if activity['snippet']['type'] == 'upload':
				videoIds.append(activity['contentDetails']['upload']['videoId'])
				publicationTimes.append(activity['snippet']['publishedAt'])
	except:
		pass
	
	return (channelId,videoIds,publicationTimes)

def getUpdate(artistId,url):
	
	if url.find('/channel/') != -1:
		uid = url[30:] #length of 'http://www.youtube.com/channel/'
		idType = 'channel'
	if url.find('/user/') != -1:
		uid = url[28:]
		idType = 'name'

	(channelId,videoIds,publicationTimes) = apiActivityGet(uid,idType)	
	print channelId + '\t' + str(videoIds) + '\t' + str(publicationTimes)


def readLinkFile(filname):
	inputs = open(filname,'rb')
	csvReadr = csv.reader(inputs, delimiter = '\t')
	csvReadr.next() #skip header
	for idx,row in enumerate(csvReadr):
		if idx < 5:
			artistId = row[0]
			url = row[1]
			update = getUpdate(artistId,url)
		else:
			break
