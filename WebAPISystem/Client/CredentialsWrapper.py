import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient
from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB

def getCredentialsClient( local = False ):
  if WebAPICS.useLocalDB():
    return CredentialsDB()
  return CredentialsClient()
