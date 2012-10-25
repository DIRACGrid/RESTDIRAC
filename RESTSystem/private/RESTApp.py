
import ssl
import sys

from tornado import web, httpserver, ioloop, process
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from RESTDIRAC.RESTSystem.Base.RESTHandler import RESTHandler
from RESTDIRAC.ConfigurationSystem.Client.Helpers import RESTConf

class RESTApp( object ):

  def __init__( self ):
    self.__handlers = {}

  def _logRequest( self, handler ):
    status = handler.get_status()
    if status < 400:
      logm = gLogger.notice
    elif status < 500:
      logm = gLogger.warn
    else:
      logm = gLogger.error
    request_time = 1000.0 * handler.request.request_time()
    logm( "%d %s %.2fms" % ( status, handler._request_summary(), request_time ) )


  def bootstrap( self ):
    gLogger.always( "\n  === Bootstrapping REST Server ===  \n" )
    ol = ObjectLoader()
    result = ol.getObjects( "RESTSystem.API", parentClass = RESTHandler, recurse = True )
    if not result[ 'OK' ]:
      return result

    self.__handlers = result[ 'Value' ]
    if not self.__handlers:
      return S_ERROR( "No handlers found" )

    self.__routes = [ ( self.__handlers[ k ].getRoute(), self.__handlers[k] ) for k in self.__handlers  ]
    gLogger.info( "Routes found:" )
    for t in sorted( self.__routes ):
      gLogger.info( " - %s : %s" % ( t[0], t[1].__name__ ) )

    balancer = RESTConf.balancer()
    kw = dict( debug = RESTConf.debug(), log_function = self._logRequest )
    if balancer and RESTConf.numProcesses not in ( 0, 1 ):
      process.fork_processes( RESTConf.numProcesses(), max_restarts = 0 )
      kw[ 'debug' ] = False
    self.__app = web.Application( self.__routes, **kw )
    port = RESTConf.port()
    if balancer:
      gLogger.notice( "Configuring REST HTTP service for balancer %s on port %s" % ( balancer, port ) )
      self.__sslops = False
    else:
      gLogger.notice( "Configuring REST HTTPS service on port %s" % port )
      self.__sslops = dict( certfile = RESTConf.cert(),
                            keyfile = RESTConf.key(),
                            cert_reqs = ssl.CERT_OPTIONAL,
                            ca_certs = RESTConf.generateCAFile() )
    self.__httpSrv = httpserver.HTTPServer( self.__app, ssl_options = self.__sslops )
    self.__httpSrv.listen( port )
    return S_OK()

  def run( self ):
    port = RESTConf.port()
    if self.__sslops:
      url = "https://0.0.0.0:%s" % port
    else:
      url = "http://0.0.0.0:%s" % port
    gLogger.always( "Starting REST server on %s" % url )
    ioloop.IOLoop.instance().start()




