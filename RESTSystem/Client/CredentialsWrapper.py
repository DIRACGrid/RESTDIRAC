import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient
from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB

__credObjs = {}

def getCredentialsClient( local = False ):
  if local or WebAPICS.useLocalDB():
    if not 'local' in __credObjs:
      __credObjs[ 'local' ] = CredentialsDB()
    return __credObjs[ 'local' ]
  if not 'remote' in __credObjs:
    __credObjs[ 'remote' ] = CredentialsClient()
  return __credObjs[ 'remote' ]
