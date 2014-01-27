# Elephantd
# Author - John Fiedler
# Company - RelateIQ
# Description - Python hadoop to statsd collector

import urllib2
from bs4 import BeautifulSoup
import statsd
import re
from time import sleep

environment = ' ' #production <fill this in>
statsd_server = ' ' #statsd.domain.com <fill this in>
dfshealth_url = ' ' #https://hadoopserver:50070/dfshealth.jsp <fill this in>
jobtracker_url = ' ' #http://hadoopserver:50030/jobtracker.jsp <fill this in>
statsd_prefix = 'ops.hadoop.' + environment

try:
	c = statsd.StatsClient(statsd_server, 8125)
except:
	print "Could not reach " + statsd_server

def print_dfs_health():
	page = urllib2.urlopen(dfshealth_url).read()
	soup = BeautifulSoup(page)
	tds = soup.find_all("td")
	i = 8
	while i < 40:
		print tds[i].text + tds[i + 1].text + tds[i + 2].text
		i = i + 3

def get_mb(text):
	if "TB" in text:
		#Convert to GB
		size = int(round(float(text.strip().replace("TB", "")) * 1024))
		return size
	elif "GB" in text:
		#Strip GB
		size = int(round(float(text.strip().replace("GB", ""))))
		return size

def strip_perc(text):
    store = round(float(text.strip().replace("%", "").replace(" ", "")), 2) / 100
    return store

def get_dfs_stats():
	try:
		page = urllib2.urlopen(dfshealth_url).read()
		soup = BeautifulSoup(page)
		c.gauge(statsd_prefix + '.dfs.capacity.GB', get_mb(soup.find('td', text=" Configured Capacity").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.dfs.used.gb', get_mb(soup.find('td', text=" DFS Used").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.dfs.not_used.gb', get_mb(soup.find('td', text=" Non DFS Used").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.dfs.remaining.gb', get_mb(soup.find('td', text=" DFS Remaining").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.dfs.used_percent', strip_perc(soup.find('td', text=" DFS Used%").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.dfs.remaining_percent', strip_perc(soup.find('td', text=" DFS Remaining%").next_sibling.next_sibling.text))
		c.gauge(statsd_prefix + '.under_replicated_blocks', soup.find('td', text=" Number of Under-Replicated Blocks").next_sibling.next_sibling.text)
		c.gauge(statsd_prefix + '.live_nodes', soup.find_all('td')[28].text) # live nodes
		c.gauge(statsd_prefix + '.dead_nodes', soup.find_all('td')[31].text) # dead nodes
		c.gauge(statsd_prefix + '.decom_nodes', soup.find_all('td')[34].text) # decom nodes
	except:
		print "Could not reach " + dfshealth_url

def print_job_stats():
	try:
		page = urllib2.urlopen(jobtracker_url).read()
		soup = BeautifulSoup(page)
		ths = soup.find_all('th')
		tds = soup.find_all('td')
		i = 0
		while i < 14:
		    print " " + ths[i].text + " " + tds[i].text
		    i = i + 1
	except:
		print "Could not reach " + jobtracker_url

def get_job_stats():
	try:
		page = urllib2.urlopen(jobtracker_url).read()
		soup = BeautifulSoup(page)
		ths = soup.find_all('th')
		tds = soup.find_all('td')
		i = 0
		while i < 14:
		    c.gauge(statsd_prefix + '.jobs.'+ths[i].text.replace(" ", ".").lower(), tds[i].text)
		    i = i + 1
	except:
		print "Could not reach " + jobtracker_url

while True:
	#print_dfs_health()
	get_dfs_stats()
	#print_job_stats()
	get_job_stats()

