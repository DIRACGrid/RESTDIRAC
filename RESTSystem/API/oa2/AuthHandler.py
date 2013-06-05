################################################################################
# $HeadURL $
################################################################################
"""
  AuthHandler to provide Oauth authentication to the DIRAC REST server
"""

__RCSID__  = "$Id$"

from RESTDIRAC.RESTSystem.Base.RESTHandler import RESTHandler, WErr, WOK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from RESTDIRAC.ConfigurationSystem.Client.Helpers.RESTConf import getCodeAuthURL

class AuthHandler( RESTHandler ):

  ROUTE = "/oauth2/auth"
  REQUIRE_ACCESS = False

  def post( self, *args, **kwargs ):
    return self.get( *args, **kwargs )

  def get( self ):
    #Auth
    args = self.request.arguments
    try:
      respType = args[ 'response_type' ][-1]
    except KeyError:
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
    elif respType in ( "token", "password" ):
      #Not implemented
      self.send_error( 501 )
    else:
      #ERROR!
      self.send_error( 400 )

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
