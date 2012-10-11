from tornado import web, gen
from RESTDIRAC.RESTSystem.API.RESTHandler import RESTHandler, WErr, WOK
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from RESTDIRAC.ConfigurationSystem.Client.Helpers.RESTConf import getCodeAuthURL

class TokenHandler( RESTHandler ):

  ROUTE = "/oauth2/token"
  REQUIRE_ACCESS = False

  class CodeGrant:

    __oaToken = OAToken()

    def __init__( self, args ):
      self.args = args
      self.error = False
      self.log = TokenHandler.getLog()
      #Look for required params
      if 'code' not in args or not 'client_id' in args:
        self.error = "invalid_request"
        return
      self.code = args[ 'code' ][0]
      self.cid = args[ 'client_id' ][0]
      #Is redirect there?
      self.redirect = False
      if 'redirect_uri' in args:
        self.redirect = args[ 'redirect_uri' ][0]

    def __str__( self ):
      if self.error:
        return "<CodeGrant error=%s>" % self.error
      return "<CodeGrant cid=%s code=%s redirect=%s>" % ( self.cid, self.code, self.redirect)

    def issueCode( self ):
      #Load client stuff
      result = self.__oaToken.getClientDataByID( self.cid )
      if not result[ 'OK' ]:
        self.log.error( "Could not retrieve client data for id %s: %s" % ( self.cid, result[ 'Message' ] ) )
        return { 'error' :  'invalid_client' }
      cData = result[ 'Value' ]
      result = self.__oaToken.generateTokenFromCode( self.cid, self.code,
                                                     redirect = self.redirect, renewable = True )
      if not result[ 'OK' ]:
        self.log.error( "Could not geneate token for %s: %s" % ( self, result[ 'Message' ] ) )
        return { 'error' : 'invalid_grant' }
      data = result[ 'Value' ]
      self.log.info( "Issued token to %s" % self )
      return { 'access_token' : data[ 'Access' ][ 'Token' ],
               'token_type' : 'bearer',
               'expires_in' : data[ 'Access' ][ 'LifeTime' ],
               'refresh_token' : data[ 'Refresh' ][ 'Token' ] }

  #POST == GET
  def post( self ):
    return self.get()

  @web.asynchronous
  @gen.engine
  def get( self ):
    args = self.request.arguments
    try:
      grant = args[ 'grant_type' ][0]
    except KeyError:
      self.send_error( 400 )
      return
    if grant == 'authorization_code':
      cg = TokenHandler.CodeGrant( args )
      if cg.error:
        self.log.error( "Auth grant error: %s" % cg.error )
        self.finish( { 'error' : cg.error } )
        return
      self.log.info( "Trying to issue token to %s" % cg )
      result = yield self.threadTask( cg.issueCode )
      self.finish( result )
      return
    elif grant == "refresh_token":
      #Not yet done :P
      pass
    self.finish( { 'error' : 'unsupported_grant_type' } )



