import bottle
import oauth2
import urlparse
import urllib
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.OAuthHelper import OAuthHelper

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
@bottle.post( "/oauth/request_token" )
@bottle.route( "/oauth/request_token" )
def oauthRequestToken():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest )

  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )

  oaData = result[ 'Value' ]

  callback = ""
  if 'callback' in oaData and str( oaData[ 'callback' ] ) != 'oob':
    callback = oaData[ 'callback' ]

  result = oaHelper.generateRequest( oaData[ 'consumer' ], callback )
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  tokenData = result[ 'Value' ]
  reqToken = oauth2.Token( tokenData[ 'request' ], tokenData[ 'secret' ] )
  reqToken.set_callback( tokenData[ 'callback' ] )

  return reqToken.to_string()

@bottle.post( "/oauth/authorize" )
@bottle.route( "/oauth/authorize" )
def oauthAuthorizeToken():
  oaRequest = getOARequest()

  print "AUTH", bottle.request.query_string

  if 'oauth_token' in oaRequest:
    webURL = "%s?oauth_token=%s" % ( oaHelper.getWebAuthorizationURL(), urllib.quote_plus( oaRequest[ 'oauth_token' ] ) )
    if 'oauth_callback' in oaRequest:
      webURL = "%s&oauth_callback=%s" % ( webURL, urllib.quote_plus( oaRequest[ 'oauth_callback' ] ) )
    gLogger.notice( "redirecting to %s" % webURL )
    bottle.redirect( webURL )

  bottle.abort( 400, "Missing request token" )

@bottle.post( "/oauth/access_token" )
@bottle.route( "/oauth/access_token" )
def oauthAccessToken():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest, checkRequest = True, checkVerifier = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  result = oaHelper.generateToken( oaData[ 'consumer' ], oaData[ 'request' ], oaData[ 'verifier' ] )
  if not result[ 'OK' ]:
    bottle.abort( 401, "Invalid verifier: %s" % result[ 'Value' ] )
  tokenData = result[ 'Value' ]

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



@bottle.route( "/oauth/echo" )
@bottle.post( "/oauth/echo" )
def echo():
  oaRequest = getOARequest()

  result = oaHelper.checkRequest( oaRequest, checkToken = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )

  data = "PARAMS : %s\n" % urllib.urlencode( bottle.request.params )
  data += "QUERY_STRING: %s\n" % bottle.request.query_string

  return data

