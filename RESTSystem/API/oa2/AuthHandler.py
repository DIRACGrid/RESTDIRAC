
from tornado import web, gen
from RESTDIRAC.RESTSystem.API.RESTHandler import RESTHandler, WErr, WOK
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from RESTDIRAC.ConfigurationSystem.Client.Helpers.RESTConf import getCodeAuthURL

class AuthHandler( RESTHandler ):

  ROUTE = "/oauth2/(auth|groups)"
  __oaToken = OAToken()

  @web.asynchronous
  @gen.engine
  def get( self, reqType ):
    #Show available groups for certificate
    if reqType == "groups":
      result = self.__getGroups()
      if not result.ok:
        self.log.error( result.msg )
        self.send_error( result.code )
        return
      self.finish( result.data )
      return

    #Auth
    args = self.request.arguments
    try:
      respType = args[ 'response_type' ][0]
    except KeyError:
      #Bad request
      self.send_error( 400 )
      return
    #Start of Code request
    if respType == "code":
      try:
        cid = args[ 'client_id' ]
      except KeyError:
        self.send_error( 400 )
        return
      self.redirect( "%s?%s" % ( getCodeAuthURL(), self.request.query ) )
    elif respType == "client_credentials":
      result = yield( self.threadTask( self.__clientCredentialsRequest ) )
      if not result.ok:
        self.log.error( result.msg )
        self.send_error( result.code )
        return
      self.finish( result.data )
    elif respType in ( "token", "password" ):
      #Not implemented
      self.send_error( 501 )
    else:
      #ERROR!
      self.send_error( 400 )



  def __codeRequest( self ):
    try:
      cid = args[ 'client_id' ]
    except KeyError:
      return WErr( 400, "Missing client_id"  )
    result = self.__oaToken.getClientDataByID( cid )
    if not result[ 'OK' ]:
      return WErr( 401, "Could not retrieve client info: %s" % result[ 'Message' ] )
    cliData = result[ 'Value' ]
    self.log.notice( "Authenticated valid client %s ( %s )" % ( cliData[ 'Name' ], cliData[ 'ClientID' ] )  )
    kw = {}
    for k in ( 'redirect_uri', 'scope', 'state' ):
      if k in args:
        value = args[k]
        if k == 'redirect_uri':
          k = 'redirect'
        kw[ k ] = value[0]
    result = self.__oaToken.generateCode( cid, **kw )
    if not result[ 'OK' ]:
      return WErr( 500, result[ 'Value' ] )
    codeData = { 'code' : result[ 'Value' ][ 'Code' ] }
    if 'state' in kw:
      codeData[ 'state' ] = kw[ 'state' ]

    return WOK( codeData )

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

  def __clientCredentialsRequest( self ):
    args = self.request.arguments
    try:
      group = args[ 'group' ][0]
    except KeyError:
      return WErr( 400, "Missing user group" )
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
    scope = False
    if 'scope' in args:
      scole = args[ 'scope' ]
    result = self.__oaToken.generateToken( DN, group, scope = scope, renewable = False )
    if not result[ 'OK' ]:
      return WErr( 500, "Error generating token: %s" % result[ 'Message' ] )
    data = result[ 'Value' ][ 'Access' ]
    res = {}
    for ki, ko in ( ( "LifeTime", "expires_in" ), ( 'Token', 'token' ) ):
      if ki in data:
        res[ ko ] = data[ ki ]

    return WOK( res )

