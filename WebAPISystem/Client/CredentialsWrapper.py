
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient
from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB

def getCredentialsClient( local = False ):
  if local:
    return CredentialsDB()
  return CredentialsClient()
