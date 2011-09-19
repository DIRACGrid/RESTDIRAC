import bottle, sys
from WebAPIDIRAC.WebAPISystem.private.routes import OAuthRoutes
from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import OAuthPlugin
import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS
from DIRAC.Core.Base import Script
from DIRAC import gLogger

def runServer():
  bottle.install( OAuthPlugin() )
  bottle.run( host = 'localhost', port = 9354, reloader = True )#, server = "flup" )

@bottle.route( "/" )
def index():
  request = bottle.request
  html = "<html><body>"
  result = Credentials.getUserDN( request )
  if not result[ 'OK']:
    html += "<h2>I don't know who you are:%s</h2>" % result[ 'Message' ]
  else:
    html += "<h2>You are %s: %s</h2>" % result['Value']
  html += "<table>"
  keys = request.keys()
  for key in keys:
    html += "<tr><td>%s</td><td>%s</td></tr>" % ( key, request[ key ] )
  html + "</body></html>"
  return html


if __name__ == "__main__":
  Script.addDefaultOptionValue( "/DIRAC/Security/UseServerCertificate", True )
  Script.parseCommandLine()
  result = WebAPICS.isOK()
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit( 1 )

  runServer()
