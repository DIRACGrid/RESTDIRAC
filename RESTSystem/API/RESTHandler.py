
from DIRAC import gLogger
from DIRAC.Core.Utilities.ThreadPool import getGlobalThreadPool
from DIRAC.Core.Security.X509Chain import X509Chain

import ssl
import sys
import tornado.web
import tornado.ioloop
import tornado.gen

class WErr( object ):
  def __init__( self, code, msg = "", **kwargs ):
    for k in kwargs:
      setattr( self, k, kwargs[ k ] )
    self.ok = False
    self.code = code
    self.msg = msg

class WOK( object ):
  def __init__( self, data = False, **kwargs ):
    for k in kwargs:
      setattr( self, k, kwargs[ k ] )
    self.ok = True
    self.data = data

class RESTHandler( tornado.web.RequestHandler ):

  ROUTE = False
  __threadPool = getGlobalThreadPool()
  __log = False

  @staticmethod
  def threadTask( method, *args, **kwargs ):
    """
    Helper method to generate a gen.Task and automatically call the callback when the real
    method ends. THIS IS SPARTAAAAAAAAAA
    """
    #Save the task to access the runner
    genTask = False

    #This runs in the separate thread, calls the callback on finish and takes into account exceptions
    def cbMethod( *cargs, **ckwargs ):
      cb = ckwargs.pop( 'callback' )
      method = cargs[0]
      cargs = cargs[1:]
      ioloop = tornado.ioloop.IOLoop.instance()
      try:
        result = method( *cargs, **ckwargs )
        ioloop.add_callback( lambda : cb( result ) )
      except Exception, excp:
        exc_info = sys.exc_info()
        ioloop.add_callback( lambda : genTask.runner.handle_exception( *exc_info ) )


    #Put the task in the thread :)
    def threadJob( tmethod, *targs, **tkwargs ):
      RESTHandler.__threadPool.generateJobAndQueueIt( cbMethod, args = ( tmethod, ) + targs, kwargs = tkwargs )
    #Return a YieldPoint
    genTask = tornado.gen.Task( threadJob, method, *args, **kwargs )
    return genTask

  def __init__( self, *args, **kwargs ):
    super( RESTHandler, self ).__init__( *args, **kwargs )
    if not RESTHandler.__log:
      RESTHandler.__log = gLogger.getSubLogger( self.__class__.__name__ )
    self.__credDict = False

  def getClientCredentials( self ):
    """
    Get info from the connecting client certificate if any
    """
    if self.__credDict or self.request.protocol != "https":
      return self.__credDict
    derCert = self.request.get_ssl_certificate( binary_form = True )
    if not derCert:
      return False

    pemCert = ssl.DER_cert_to_PEM_cert( derCert )
    cert = X509Chain()
    cert.loadChainFromString( pemCert )
    result = cert.getCredentials()
    if not result[ 'OK' ]:
      self.log.error( "Could not get client credentials: %s" % result[ 'Message' ] )
      return False
    self.__credDict = result[ 'Value' ]
    return self.__credDict

  @property
  def log( self ):
    return self.__log

  @classmethod
  def getLog( cls ):
    return cls.__log


  @classmethod
  def getRoute( cls ):
    """
    Return the route for the Handler
    """
    return cls.ROUTE