
from tornado import web, gen
from DIRAC import gConfig
from RESTDIRAC.RESTSystem.Base.RESTHandler import RESTHandler, WErr, WOK
from RESTDIRAC.RESTSystem.Client.OAToken import OAToken
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


