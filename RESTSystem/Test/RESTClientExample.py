# Apache HTTP requests manipulation module
import requests
import json
import os

# The REST server url
REST_URL = 'https://ccdirac06.in2p3.fr:9178'

###########################################
# Get the access token first

# GET request parameters
params = {'response_type':'client_credentials',
          'group':'dirac_user',
          'setup':'Dirac-Production'} 

# The user certificate, password will be asked for to the user
# before request submission
certificate = ('/Users/atsareg/.globus/usercert.pem', 
               '/Users/atsareg/.globus/userkey.pem')

result = requests.get(REST_URL+'/oauth2/auth',params=params,cert=certificate,verify=False)

# the output is returned as a json encoded string, decode it here
resultDict = json.loads( result.text )
access_token = resultDict['token']

###################################
#### Submit a job
###################################

####################################################
# Prepare the job description ( manifest ) first

manifest = { 
  'Executable': '/bin/cat',
  'Arguments': 'rest_test.py rest1_test.py',
  'StdOut' : 'std.out',
  'StdError' : 'std.err',
  'OutputSandbox' : ['std.out','std.err'],
  'JobName' : 'REST_test'
}

# add json encoded job manifest to the data to be transfered
# to the REST server
data = { 'manifest' : json.dumps( manifest ) }

##############################################################
# Input sandbox files as Multipart-Encoded files

files = { 'file' : ('rest_test.py', open('rest_test.py','rb') ),
          'file1' : ('rest1_test.py', open('rest1_test.py','rb') ) }

###############################################################
#  Submit the job now, POST http request creates a new job,
#  from now on access_token should be passed as the request parameter
#  verify=False is to not to verify the server certificate by the client

result = requests.post(REST_URL+'/jobs',
                       data=data,
                       files = files,
                       params={'access_token':access_token},
                       verify=False)
resultDict = json.loads( result.text )

# resulting job ID(s) are returned as a list ( e.g. when bulk submission ) 
jobID = resultDict['jids'][0]

########################################
# Get job status

result = requests.get(REST_URL+'/jobs/%d' % jobID, 
                      params={'access_token':access_token},
                      verify=False)
resultDict = json.loads( result.text )

print "Status for job %d:" % jobID, resultDict['status']

###############################################
# Get job OutputSandbox for some already executed jobs,
# the outputsandbox files are returned as a single tar
# gzipped archive  

jobID = 4124463
result = requests.get(REST_URL+'/jobs/%d/outputsandbox' % jobID, 
                      params={'access_token':access_token},
                      verify=False)

# Write the output sandbox gzipped tar archive to disk
tmpOutputName = 'tmp_%d.tar.gz' % jobID
outputFile = open(tmpOutputName,'w')
outputFile.write(result.content)
outputFile.close()

# untar it finally
os.system('tar xzf %s' % tmpOutputName)
os.remove( tmpOutputName )