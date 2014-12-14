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
import dateutil.parser
# AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI
youtubeKey = 'AIzaSyDeZXABKSQATB-LjcxeOw3M3ppInSjEwzc'
youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=" + youtubeKey + "&q="#"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=" + youtubeKey + "&channelId=" #AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI"

def apiActivityGet(uid,idType):
	
	# build api url
	if idType == 'name':
		url = youtubeGetChannelIdUrl
		url += uid
		# print url

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

	if channelId == None:
		return ('',[],[])
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
	titles = []
	# print response
	try:
		#parse response body
		for activity in response['items']:
			# print activity
			if activity['snippet']['type'] == 'upload':
				videoIds.append(activity['contentDetails']['upload']['videoId'])
				publicationTimes.append(activity['snippet']['publishedAt'])
				titles.append(activity['snippet']['title'])
	except:
		pass
	
	return (channelId,videoIds,publicationTimes,titles )

def getUpdate(artistId,url):
	
	if url.find('/channel/') != -1:
		uid = url[30:] #length of 'http://www.youtube.com/channel/'
		idType = 'channel'
	if url.find('/user/') != -1:
		uid = url[28:]
		idType = 'name'
	else:
		uid = url[23:]
		idType = 'name'

	(channelId,videoIds,publicationTimes,titles) = apiActivityGet(uid,idType)	
	# print channelId + '\t' + str(videoIds) + '\t' + str(publicationTimes)
	return artistId, channelId, videoIds, publicationTimes,titles

outputs = open('youtubeUpdate.txt','wb')
csvWriter = csv.writer(outputs, delimiter = '\t')
csvWriter.writerow(['artistId','channelId','Recently Uploaded Videos','Upload Times'])

def readLinkFile(filname):
	inputs = open(filname,'rb')
	outputs = open('youtubeUpdate.json','wb')	

	csvReadr = csv.reader(inputs, delimiter = '\t')
	csvReadr.next() #skip header

	
	num_cores = multiprocessing.cpu_count()- 1
	outputDict = {}

	# results = Parallel(n_jobs=num_cores)(delayed(updateAndwrite)(i,idx,outputDict) for idx,i in enumerate(csvReadr))  
	for idx,row in enumerate(csvReadr):
		# print idx
		if idx < 5:
			updateAndwrite(row,idx,outputDict)
		else:
			break
	inputs.close()
	# outputs.close()
	srted = sorted(outputDict.items())
	# print srted
	finDict = {}
	for item in srted:
		print item[0]
		finDict[item[0]] = outputDict[item[0]]
	
	json.dump(finDict,outputs)
	outputs.close()
	return (finDict,outputDict)

def updateAndwrite(row,idx,outputDict):
	print idx
	artistId = row[0]
	url = row[1]
	(artistId, channelId, videoIds, publicationTimes,titles) = getUpdate(artistId,url)
	# csvWriter.writerow(update)
	for idx,video in enumerate(videoIds):
		timeUp = publicationTimes[idx]
		formatedTime = dateutil.parser.parse(timeUp)
		dat = str(formatedTime.year)  + '-'+str(formatedTime.month) + '-'+ str(formatedTime.day)
		videoObject = {}
		videoObject['videoId'] = video
		videoObject['time'] = str(formatedTime.hour) + ':'+str(formatedTime.minute) + ':'+str(formatedTime.second) 
		videoObject['artistId'] = artistId
		videoObject['title'] = titles[idx]
		videoObject['channelId'] = channelId

		if dat in outputDict:
			outputDict[dat].append(videoObject)
		else:
			outputDict[dat] = []
			outputDict[dat].append(videoObject)
	print outputDict
	return 'done'