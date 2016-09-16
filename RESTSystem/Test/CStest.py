import requests
import json
import os

# The REST server url
REST_URL = 'https://0.0.0.0:9910'

###########################################
# Get the access token first

# GET request parameters
params = {'grant_type':'client_credentials',
          'group':'TrustedHost',
          'setup':'LHCb-Certification'}

# The user certificate, password will be asked for to the user
# before request submission
#certificate = ('/home/cinzia/.globus/usercert.pem',
#               '/home/cinzia/.globus/userkey.pem')


certificate = ('/home/cinzia/devRoot/etc/grid-security/hostcert.pem','/home/cinzia/devRoot/etc/grid-security/hostkey.pem')
proxies=('/tmp/x509up_u1000','/tmp/x509up_u1000')

#result = requests.get(REST_URL+"/oauth2/token",params=params,cert=proxies, verify=False)
result = requests.get(REST_URL+"/oauth2/token",params=params,cert=certificate,verify=False)


# the output is returned as a json encoded string, decode it here
resultDict = json.loads( result.text )
access_token = resultDict['token']

JobHistory = requests.get(REST_URL+'/jobs/history',params={'access_token':access_token},
                      verify=False)

PilotCommands = requests.get(REST_URL+'/config/Value',params={'access_token':access_token,'ValuePath':'/Operations/LHCb-Certification/Pilot/Commands/BOINC'}, verify=False)

########################################
