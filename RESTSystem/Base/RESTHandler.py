
from DIRAC import gLogger
from DIRAC.Core.Utilities.ThreadPool import getGlobalThreadPool
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
from RESTDIRAC.ConfigurationSystem.Client.Helpers import RESTConf

import ssl
import functools
import sys
import os
import tempfile
import shutil
import tornado.web
import tornado.ioloop
import tornado.gen
import tornado.stack_context

class WErr( tornado.web.HTTPError ):
  def __init__( self, code, msg = "", **kwargs ):
    super( WErr, self ).__init__( code, msg or None )
    for k in kwargs:
      setattr( self, k, kwargs[ k ] )
    self.ok = False
    self.msg = msg
    self.kwargs = kwargs

  def __str__( self ):
    return super( tornado.web.HTTPError, self ).__str__()

  @staticmethod
  def fromError( res, code = 500 ):
    return WErr( code = code, msg = res[ 'Message' ] )


class WOK( object ):
  def __init__( self, data = False, **kwargs ):
    for k in kwargs:
      setattr( self, k, kwargs[ k ] )
    self.ok = True
    self.data = data

class TmpDir( object ):
  def __init__( self ):
    self.__tmpDir = False
  def __enter__( self ):
    return self.get()
  def __exit__( self, *exc_info ):
    try:
      shutil.rmtree( self.__tmpDir )
      self.__tmpDir = False
    except:
      pass
  def get( self ):
    if not self.__tmpDir:
      base = os.path.join( RESTConf.getWorkDir(), "tmp" )
      if not os.path.isdir( base ):
        try:
          os.makedirs( base )
        except Exception, e:
          gLogger.exception( "Cannot create work dir %s: %s" % ( base, e) )
          raise
      self.__tmpDir = tempfile.mkdtemp( dir = base )
    return self.__tmpDir


def asyncThreadTask( method ):
  """
  Helper for async decorators
  """
  return tornado.web.asynchronous( tornado.gen.engine( method ) )

class RESTHandler( tornado.web.RequestHandler ):

  ROUTE = False
  REQUIRE_ACCESS = True
  __threadPool = getGlobalThreadPool()
  __disetConfig = ThreadConfig()
  __oaToken = OAToken()
  __log = False

  #Helper function to create threaded gen.Tasks with automatic callback and exception handling
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
      disetConf = cargs[1]
      cargs = cargs[2]
      RESTHandler.__disetConfig.load( disetConf )
      ioloop = tornado.ioloop.IOLoop.instance()
      try:
        result = method( *cargs, **ckwargs )
        ioloop.add_callback( functools.partial( cb, result ) )
      except Exception, excp:
        exc_info = sys.exc_info()
        ioloop.add_callback( lambda : genTask.runner.handle_exception( *exc_info ) )


    #Put the task in the thread :)
    def threadJob( tmethod, *targs, **tkwargs ):
      tkwargs[ 'callback' ] = tornado.stack_context.wrap( tkwargs[ 'callback' ] )
      targs = ( tmethod, RESTHandler.__disetConfig.dump(), targs )
      RESTHandler.__threadPool.generateJobAndQueueIt( cbMethod, args = targs, kwargs = tkwargs )
    #Return a YieldPoint
    genTask = tornado.gen.Task( threadJob, method, *args, **kwargs )
    return genTask

  #END OF THREAD TASK


  def __init__( self, *args, **kwargs ):
    super( RESTHandler, self ).__init__( *args, **kwargs )
    if not RESTHandler.__log:
      RESTHandler.__log = gLogger.getSubLogger( self.__class__.__name__ )
    self.__credDict = False
    self.__uData = False

  def __getAccessToken( self ):
    """
    Try to find the access token in the arguments or header
    """
    args = self.request.arguments
    if 'access_token' in args:
      return args[ 'access_token' ][0]
    elif 'Authorization' in self.request.headers:
      auth = self.request.headers[ 'Authorization' ].split()
      if len( auth ) == 2 and auth[0].lower() == "bearer":
        return auth[1]
    return False

  @tornado.gen.engine
  def prepare( self ):
    """
    Prepare the request
    """
    self._auto_run = False
    self.set_header( "Pragma", "no-cache" )
    self.set_header( "Cache-Control", "no-store" )
    RESTHandler.__disetConfig.reset()
    self.accessToken = self.__getAccessToken()
    if self.accessToken:
      result = yield self.threadTask( self.__oaToken.getCachedToken, self.accessToken )
      if not result[ 'OK' ]:
        self.log.info( "Invalid access token!" )
        self.send_error( 401 )
      else:
        data = result[ 'Value' ]
        if data[ 'UserGroup' ] == 'TrustedHost':
          data[ 'UserGroup' ] = 'hosts'
        self.__uData = { 'DN' : data[ 'UserDN' ],
                         'username' : data[ 'UserName' ],
                         'group' : data[ 'UserGroup' ],
                         'setup' : data[ 'UserSetup' ] }
        RESTHandler.__disetConfig.setDN( data[ 'UserDN' ] )
        RESTHandler.__disetConfig.setGroup( data[ 'UserGroup' ] )
        RESTHandler.__disetConfig.setSetup( data[ 'UserSetup' ] )
        cs = " ".join( [ "%s=%s" % ( k, self.__uData[k] ) for k in sorted( self.__uData ) ] )
        self.log.info( "Setting DISET for %s" % cs )
    elif self.REQUIRE_ACCESS:
      raise WErr( 401, "No token provided" )
    self.end_prepare()


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

  def getUserDN( self ):
    if self.__uData:
      return self.__uData[ 'DN' ]
    return False

  def getUserName( self ):
    if self.__uData:
      return self.__uData[ 'username' ]
    return False

  def getUserGroup( self ):
    if self.__uData:
      return self.__uData[ 'group' ]
    return False

  #START TORNADO HACK
  def _execute(self, transforms, *args, **kwargs):
    """Executes this request with the given output transforms."""
    self._auto_run = True
    self._transforms = transforms
    try:
      if self.request.method not in self.SUPPORTED_METHODS:
        raise HTTPError(405)
      # If XSRF cookies are turned on, reject form submissions without
      # the proper cookie
      if self.request.method not in ("GET", "HEAD", "OPTIONS") and \
         self.application.settings.get("xsrf_cookies"):
        self.check_xsrf_cookie()
      self._method_args = ( args, kwargs )
      self.prepare()
      if self._auto_run:
        self.end_prepare()
    except Exception, e:
      self._handle_request_exception(e)

  def end_prepare( self ):
    self._prepared = True
    try:
      args, kwargs = self._method_args
      if not self._finished:
        args = [self.decode_argument(arg) for arg in args]
        kwargs = dict((k, self.decode_argument(v, name=k))
                      for (k, v) in kwargs.iteritems())
        getattr(self, self.request.method.lower())(*args, **kwargs)
        if self._auto_finish and not self._finished:
            self.finish()
    except Exception, e:
      self._handle_request_exception(e)
