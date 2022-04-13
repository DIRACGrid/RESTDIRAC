""" User access token handler
"""

from tornado import web, gen
from DIRAC import gConfig
from RESTDIRAC.RESTSystem.Base.RESTHandler import RESTHandler, WErr, WOK
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

class TokenHandler( RESTHandler ):

  ROUTE = "/oauth2/(token|groups|setups)"
  REQUIRE_ACCESS = False

  __oaToken = OAToken()

  class CodeGrant:

    def __init__( self, args, oaToken ):
      self.args = args
      self.oaToken = oaToken
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
      result = self.oaToken.getClientDataByID( self.cid )
      if not result[ 'OK' ]:
        self.log.error( "Could not retrieve client data for id %s: %s" % ( self.cid, result[ 'Message' ] ) )
        return { 'error' :  'invalid_client' }
      cData = result[ 'Value' ]
      result = self.oaToken.generateTokenFromCode( self.cid, self.code,
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
  def post( self, *args, **kwargs ):
    return self.get( *args, **kwargs )

  @web.asynchronous
  def get( self, reqType ):
    #Show available groups for certificate
    if reqType == "groups":
      return self.groupsAction()
    elif reqType == "setups":
      return self.setupsAction()
    elif reqType == "token":
      return self.tokenAction()

  def __getGroups( self, DN = False ):
    if not DN:
      credDict = self.getClientCredentials()
      if not credDict:
        return WErr( 401, "No certificate received to issue a token" )
      DN = credDict[ 'subject' ]
      if not credDict[ 'validDN' ]:
       return WErr( 401, "Unknown DN %s" % DN )
    result = Registry.getGroupsForDN( DN )
    if not result[ 'OK' ]:
      return WErr( 500, result[ 'Message' ] )
    return WOK( { 'groups' : result[ 'Value' ] } )

  def groupsAction( self ):
      result = self.__getGroups()
      if not result.ok:
        self.log.error( result.msg )
        self.send_error( result.code )
        return
      self.finish( result.data )
      return

  def setupsAction( self ):
    self.finish( { 'setups' : gConfig.getSections( "/DIRAC/Setups" )[ 'Value' ] } )

  @gen.engine
  def tokenAction( self ):
    args = self.request.arguments
    try:
      grant = args[ 'grant_type' ][0]
    except KeyError:
      self.send_error( 400 )
      return
    if grant == 'authorization_code':
      cg = TokenHandler.CodeGrant( args, self.__oaToken )
      if cg.error:
        self.log.error( "Auth grant error: %s" % cg.error )
        self.finish( { 'error' : cg.error } )
        return
      self.log.info( "Trying to issue token to %s" % cg )
      result = yield self.threadTask( cg.issueCode )
      self.finish( result )
      return
    if grant == "client_credentials":
      result = yield( self.threadTask( self.__clientCredentialsRequest ) )
      if not result.ok:
        self.log.error( result.msg )
        raise result
      self.finish( result.data )
      return
    elif grant == "refresh_token":
      #Not yet done :P
      pass
    self.finish( { 'error' : 'unsupported_grant_type' } )

  def __clientCredentialsRequest( self ):
    args = self.request.arguments
    try:
      group = args[ 'group' ][0]
    except KeyError:
      return WErr( 400, "Missing user group" )
    try:
      setup = args[ 'setup' ][0]
    except KeyError:
      return WErr( 400, "Missing setup" )
    credDict = self.getClientCredentials()
    if not credDict:
      return WErr( 401, "No certificate received to issue a token" )
    DN = credDict[ 'subject' ]
    if not credDict[ 'validDN' ]:
      return WErr( 401, "Unknown DN %s" % DN )
    #Check group
    result = self.__getGroups( DN )
    if not result.ok:
      return result
    groups = result.data[ 'groups' ]
    if group not in groups:
      return WErr( 401, "Invalid group %s for %s (valid %s)" % ( group, DN, groups ) )
    if setup not in gConfig.getSections( "/DIRAC/Setups" )[ 'Value' ]:
      return WErr( 401, "Invalid setup %s for %s" % ( setup, DN ) )
    scope = False
    if 'scope' in args:
      scope = args[ 'scope' ]
    result = self.__oaToken.generateToken( DN, group, setup, scope = scope, renewable = False )
    if not result[ 'OK' ]:
      return WErr( 500, "Error generating token: %s" % result[ 'Message' ] )
    data = result[ 'Value' ][ 'Access' ]
    res = {}
    for ki, ko in ( ( "LifeTime", "expires_in" ), ( 'Token', 'token' ) ):
      if ki in data:
        res[ ko ] = data[ ki ]

    return WOK( res )
