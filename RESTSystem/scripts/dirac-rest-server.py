
from DIRAC.Core.Base import Script
from DIRAC import gLogger

from tornado import web, httpserver, ioloop
import ssl


if __name__ == "__main__":
  from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
  from RESTDIRAC.RESTSystem.API.RESTHandler import RESTHandler
  from DIRAC.Core.Security import Locations



  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'true' )
  Script.localCfg.addDefaultEntry( "LogLevel", "INFO" )
  #Script.enableCS()
  Script.disableCS()
  Script.parseCommandLine()


  ol = ObjectLoader()
  result = ol.getObjects( "RESTSystem.API", parentClass = RESTHandler, recurse = True )
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit(1)
  handlers = result[ 'Value' ]
  if not handlers:
    gLogger.fatal( "No handlers found" )
    sys.exit( 1 )

  handlers = dict( ( handlers[ k ].getRoute(), handlers[k ] ) for k in handlers  )
  handlers = [ ( k, handlers[k] ) for k in handlers ]

  app = web.Application( handlers, debug = True )
  loc = Locations.getHostCertificateAndKeyLocation()
  sslops = { "certfile" : loc[0], "keyfile" : loc[1], "cert_reqs" : ssl.CERT_OPTIONAL,
             "ca_certs" : "/Users/adria/Devel/diracRoot/etc/grid-security/allCAs.pem" }
  https = httpserver.HTTPServer( app, ssl_options = sslops )
  https.listen( 10000 )
  gLogger.notice( "Starting REST server on port 10000" )
  ioloop.IOLoop.instance().start()


