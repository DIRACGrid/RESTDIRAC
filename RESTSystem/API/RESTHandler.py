
from DIRAC import gLogger

import tornado.web


class RESTHandler( tornado.web.RequestHandler ):

  ROUTE = False

  @staticmethod
  def asyncTask( method, *args, **kwargs ):
    """
    Helper method to generate a gen.Task and automatically call the callback when the real
    method ends
    """
    def cbMethod( *inArgs, **inKwargs ):
      cb = inKwargs.pop( 'callback' )
      cb( method( *inArgs, **inKwargs ) )
    return tornado.gen.Task( cbMethod, *args, **kwargs )


  def __init__( self, *args, **kwargs ):
    super( RESTHandler, self ).__init__( *args, **kwargs )
    self.__log = gLogger.getSubLogger( self.__class__.__name__ )

  @property
  def log( self ):
    return self.__log

  @classmethod
  def getRoute( cls ):
    """
    Return the route for the Handler
    """
    return cls.ROUTE