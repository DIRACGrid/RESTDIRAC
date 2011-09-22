import bottle, sys


from DIRAC.Core.Base import Script
from DIRAC import gLogger


@bottle.route( "/" )
def index():
  html = "<html><body>Howdy cowboy!</body></html>"
  return html


if __name__ == "__main__":
  import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS
  from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import OAuthPlugin
  from WebAPIDIRAC.WebAPISystem.private.RouteLoader import loadRoutes

  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'true' )
  Script.localCfg.addDefaultEntry( "LogLevel", "INFO" )
  Script.enableCS()
  Script.parseCommandLine()
  result = WebAPICS.isOK()
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit( 1 )

  loadRoutes()
  bottle.install( OAuthPlugin() )
  bottle.run( host = 'localhost', port = 9354, reloader = True )#, server = "flup" )
