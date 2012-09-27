
from tornado import web, gen
from RESTDIRAC.RESTSystem.API.RESTHandler import RESTHandler
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken

class AuthHandler( RESTHandler ):

  ROUTE = "/oa2/auth"
  __oaToken = OAToken()

  @web.asynchronous
  @gen.engine
  def get( self ):
    try:
      respType = self.request.arguments[ 'response_type' ][0]
    except KeyError:
      #Bad request
      self.send_error( 400 )
      return
    #Start of Code request
    if respType == "code":
      result = yield self.asyncTask( self.__codeRequest )
      if result[ 0 ] != 200:
        self.send_error( result[ 0 ] )
        return
      self.finish( str( result ) )
      return

    if respType in ( "token", "password" ):
      #Not implemented
      self.send_error( 501 )
      return

    self.send_error( 100 )

  def __codeRequest( self ):
    args = self.request.arguments
    try:
      cid = args[ 'client_id' ]
    except KeyError:
      return ( 400, )
    result = self.__oaToken.getClientDataByID( cid )
    if not result[ 'OK' ]:
      self.log.error( "Could not retrieve client info: %s" % result[ 'Message' ] )
      return ( 401, )
    self.log.info( "OK!" )
    return ( 200, "OKOK" )


