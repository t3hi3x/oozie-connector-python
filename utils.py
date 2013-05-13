# Copyright 2012 Alex Breshears.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__info__ = '''
	Hadoop oozie rest API wrapper. Supports version 1 of Oozie's rest API.
'''

import urllib2
import simplejson as json
import config
from urllib2 import HTTPError

class OozieConnectorException(Exception):
	pass

class BetterHTTPErrorProcessor(urllib2.BaseHandler):
	# a substitute/supplement to urllib2.HTTPErrorProcessor
	# that doesn't raise exceptions on status codes 201,204,206
	# Def a hack
	def http_error_201(self, request, response, code, msg, hdrs):
		return response
	def http_error_204(self, request, response, code, msg, hdrs):
		return response
	def http_error_206(self, request, response, code, msg, hdrs):
		return response

#TODO: Oozie: Support Basic HTTP Auth for Requests

def getWorkflowJobInfo(job_name):
	"""
		Gets job information from Oozie.
		See http://archive.cloudera.com/cdh4/cdh/4/oozie/WebServicesAPI.html#Job_Information
	"""
	url = config.OOZIE_HOST + ':' + unicode(config.OOZIE_PORT) + '/oozie/v1/job/' + job_name + '?show=info'
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	return json.loads(response.read())

def getWorkflowJobLog(job_name):
	"""
		Gets job log info from Oozie.
		See http://archive.cloudera.com/cdh4/cdh/4/oozie/WebServicesAPI.html#Job_Information
	"""
	url = config.OOZIE_HOST + ':' + unicode(config.OOZIE_PORT) + '/oozie/v1/job/' + job_name + '?show=log'
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	return response.read()

def submitWorkflowJob(xml, auto_start=True):
	"""
		Submits an XML job to Oozie.
		See http://archive.cloudera.com/cdh4/cdh/4/oozie/WebServicesAPI.html#Job_Submission
	"""
	url = config.OOZIE_HOST + ':' + unicode(config.OOZIE_PORT) + '/oozie/v1/jobs'
	if auto_start:
		url += '?action=start'
	opener = urllib2.build_opener(BetterHTTPErrorProcessor)
	urllib2.install_opener(opener)
	try:
		print url
		request = urllib2.Request(url, data=xml, headers={'Content-type': 'application/xml'})
		response = urllib2.urlopen(request)
		return json.loads(response.read())
	except HTTPError, e:
		if e.code == 400:
			raise OozieConnectorException(e.headers['oozie-error-message'])
		else:
			return None
