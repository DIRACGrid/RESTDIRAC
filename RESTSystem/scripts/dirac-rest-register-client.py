import sys
from DIRAC import S_OK, S_ERROR, gLogger
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
from DIRAC.Core.Base import Script

class Params:
  def __init__( self ):
    self.cid = ""
    self.name = ""
    self.redirect = ""
    self.url = ""
    self.icon = ""
    self.disableLocal = False

  def setClientName( self, value ):
    self.name = value
    return S_OK()
  def setClientRedirect( self, value ):
    self.redirect = value
    return S_OK()
  def setClientIcon( self, value ):
    self.icon = value
    return S_OK()
  def setClientURL( self, value ):
    self.url = value
  def setClientID( self, value ):
    self.cid = value
    return S_OK()
  def disableLocal( self, value ):
    self.disableLocal = True
    return S_OK()
  def isOK( self ):
    for k in ( 'name', 'redirect' ):
      if not getattr( self, k ):
        gLogger.error( "Missing client %s" % k )
        return False
    return True

def registerClient( params ):
  gLogger.notice( "Requesting new client" )
  oatoken = OAToken()
  oatoken._sDisableLocal = params.disableLocal
  result = oatoken.registerClient( params.name, params.redirect, params.url, params.icon )
  if not result[ 'OK' ]:
    gLogger.error( "Could not register a new client", result[ 'Message' ] )
    return False
  data = result[ 'Value' ]
  gLogger.notice( "New client:" )
  for k in sorted( data ):
    if data[ k ]:
      gLogger.notice( "\t %s = %s" % ( k.capitalize().ljust( 10 ), data[k] ) )
  return True


if __name__ == "__main__":
  #Script.addDefaultOptionValue( "/DIRAC/Security/UseServerCertificate", True )
  params = Params()
  Script.registerSwitch( "n:", "name=",
                         "Client name ",
                         params.setClientName )
  Script.registerSwitch( "b:", "redirect=",
                         "Client redirect for after authorization",
                         params.setClientRedirect )
  Script.registerSwitch( "u:", "url=",
                         "Client web information URL",
                         params.setClientURL )
  Script.registerSwitch( "i:", "icon=",
                         "Client icon to show",
                         params.setClientIcon )
  Script.registerSwitch( "a", "remote",
                         "Disable direct access to the DB",
                         params.disableLocal )
  Script.parseCommandLine()
  if not params.isOK():
    print
    print Script.showHelp()
    sys.exit( 1 )
  registerClient( params )
