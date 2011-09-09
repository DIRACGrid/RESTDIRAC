import bottle
import oauth2
import urlparse
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private import OAuthHelper

bottle.app().catchall = False

def getUserDN( environ ):
  userDN = False
  if 'HTTPS' not in environ or environ[ 'HTTPS' ] != 'on':
    gLogger.info( "Getting the DN from /OAuth/DebugDN" )
    userDN = gConfig.getValue( "/OAuth/DebugDN", "/yo" )
  elif 'SSL_CLIENT_S_DN' in environ:
    userDN = environ[ 'SSL_CLIENT_S_DN' ]
  elif 'SSL_CLIENT_CERT' in environ:
    userCert = X509Certificate.X509Certificate()
    result = userCert.loadFromString( environ[ 'SSL_CLIENT_CERT' ] )
    if not result[ 'OK' ]:
      errMsg = "Could not load SSL_CLIENT_CERT: %s" % result[ 'Message' ]
      gLogger.error( errMsg )
      return S_ERROR( errMsg )
    else:
      userDN = userCert.getSubjectDN()[ 'Value' ]
  else:
    errMsg = "Web server is not properly configured to get SSL_CLIENT_S_DN or SSL_CLIENT_CERT in env"
    gLogger.fatal( errMsg )
    return S_ERROR( errMsg )

  result = Registry.getUsernameForDN( userDN )
  if not result[ 'OK' ]:
    gLogger.info( "Could not get username for DN %s: %s" % ( userDN, result[ 'Message' ] ) )
    return result
  userName = result[ 'Value' ]
  gLogger.info( "Got username for user" " => %s for %s" % ( userName, userDN ) )
  return S_OK( ( userDN, userName ) )

def getOARequest():
  request = bottle.request

  urlparts = request.urlparts
  url = urlparse.urlunsplit( ( urlparts[0], urlparts[1], urlparts[2], "", "" ) )

  oaRequest = oauth2.Request.from_request( request.method,
                                           url,
                                           headers = request.headers,
                                           parameters = request.params,
                                           query_string = request.query_string )
  return oaRequest

oaHelper = OAuthHelper()

#Oauth flow
@bottle.post( "/oauth/token/request" )
@bottle.route( "/oauth/token/request" )
def oauthRequestToken():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]
  tokenPair = oaHelper.generateRequest( oaData[ 'consumer' ] )
  reqToken = oauth2.Token( tokenPair[0], tokenPair[1] )
  return reqToken.to_string()

@bottle.post( "/oauth/token/authorize" )
@bottle.route( "/oauth/token/authorize" )
def oauthAuthorizeToken():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest, checkRequest = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  #TODO: Missing userDN and userGROUp
  #TODO: This has to be done in the web
  return oaHelper.generateRequestVerifier( oaData[ 'consumer' ], oaData[ 'request' ] )

@bottle.post( "/oauth/token/access" )
@bottle.route( "/oauth/token/access" )
def oauthAccessToken():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest, checkRequest = True, checkVerifier = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  tokenData = oaHelper.generateToken( oaData[ 'consumer' ], oaData[ 'token' ], oaData[ 'verifier' ] )
  reqToken = oauth2.Token( tokenData[ 'token'], tokenData[ 'secret' ] )
  return reqToken.to_string()

@bottle.post( "/oauth/rawRequest" )
@bottle.route( "/oauth/rawRequest" )
def oauthRawRequest():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest, checkToken = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  #TODO: DO STUFF
  return "STUFF"


@bottle.route( "/oauth/echo" )
@bottle.post( "/oauth/echo" )
def echo():
  return bottle.request.query_string

