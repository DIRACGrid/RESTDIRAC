import bottle
import sys
from DIRAC import S_OK, S_ERROR, gLogger
from WebAPIDIRAC.WebAPISystem.Client.CredentialsWrapper import getCredentialsClient
from DIRAC.Core.Base import Script

class Params:
  def __init__( self ):
    self.consumerKey = ""
    self.name = ""
    self.callback = ""
    self.icon = ""
    self.localAccess = False
  def setConsumerName( self, value ):
    self.name = value
    return S_OK()
  def setConsumerCallback( self, value ):
    self.callback = value
    return S_OK()
  def setConsumerIcon( self, value ):
    self.icon = value
    return S_OK()
  def setConsumerKey( self, value ):
    self.consumerKey = value
    return S_OK()
  def setLocal( self, value ):
    self.localAccess = True
    return S_OK()
  def getClient( self ):
    return getCredentialsClient( self.localAccess )
  def isOK( self ):
    if not self.name:
      gLogger.error( "Missing consumer name" )
      return False
    if not self.callback:
      gLogger.error( "Missing consumer callback" )
      return False
    return True

def registerConsumer( params ):
  credClient = params.getClient()
  if params.consumerKey:
    gLogger.notice( "Requesting new consumer with key %s" % params.consumerKey )
  else:
    gLogger.notice( "Requesting new consumer" )
  result = credClient.generateConsumerPair( params.name, params.callback, params.icon, params.consumerKey )
  if not result[ 'OK' ]:
    gLogger.error( "Could not register a new consumer", result[ 'Message' ] )
    return False
  gLogger.notice( "New consumer:\n\tKey    %s\n\tSecret %s" % result[ 'Value' ] )
  return True


if __name__ == "__main__":
  #Script.addDefaultOptionValue( "/DIRAC/Security/UseServerCertificate", True )
  params = Params()
  Script.registerSwitch( "n:", "name=",
                         "Consumer name ",
                         params.setConsumerName )
  Script.registerSwitch( "b:", "callback=",
                         "Consumer callback for after authorization",
                         params.setConsumerCallback )
  Script.registerSwitch( "i:", "icon=",
                         "Consumer icon to show",
                         params.setConsumerIcon )
  Script.registerSwitch( "k:", "key=",
                         "Consumer key to use, if not specified a random one will be generated",
                         params.setConsumerKey )
  Script.registerSwitch( "a", "DBAccess",
                         "Use direct access to the DB",
                         params.setLocal )
  Script.parseCommandLine()
  if not params.isOK():
    sys.exit( 1 )
  registerConsumer( params )
