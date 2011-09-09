import bottle
import oauth2
import urlparse
from DIRAC import S_OK, S_ERROR, gLogger
from WebAPIDIRAC.private.OAuthDataStore import OAuthDataStore

gOADataStore = OAuthDataStore()

oaServer = oauth2.Server()
oaServer.add_signature_method( oauth2.SignatureMethod_HMAC_SHA1() )
oaServer.add_signature_method( oauth2.SignatureMethod_PLAINTEXT() )

bottle.debug( True )
bottle.app().catchall = False

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

def checkRequest( oaRequest, checkRequestToken = False, checkAccessToken = False, checkVerifier = False ):
  consumerKey = oaRequest[ 'oauth_consumer_key' ]
  expectedSecret = gOADataStore.getConsumerSecret( consumerKey )
  if not expectedSecret:
    return S_ERROR( "Unknown consumer key" )
  oaConsumer = oauth2.Consumer( consumerKey, expectedSecret )

  oaData = {}
  oaData[ 'consumer' ] = consumerKey

  oaToken = False

  if checkRequestToken or checkAccessToken:
    if 'oauth_token' not in oaRequest:
      return S_ERROR( "No token in request " )
    tokenString = oaRequest[ 'oauth_token' ]
    if checkRequestToken:
      checkRequest = True
      tokenType = "request"
    else:
      checkRequest = False
      tokenType = "access"
    expectedSecret = gOADataStore.getTokenSecret( consumerToken, tokenString, checkRequest )
    if not expectedSecret:
      return S_ERROR( "Unknown %s token" % tokenType )
    oaToken = oauth2.Token( tokenString, expectedSecret )
    oaData[ tokenType ] = tokenString

  try:
    oaServer.verify_request( oaRequest, oaConsumer, oaToken )
  except oauth2.Error, e:
    return S_ERROR( "Invalid request: %s" % e )

  if checkVerifier:
    if oaRequest['oauth_verifier'] != gOADataStore.getRequestVerifier( consumerToken, reqToken ):
      return S_ERROR( "Invalid verifier" )
    oaData[ 'verify' ] = oaRequest['oauth_verifier']

  return S_OK( oaData )



#Oauth flow
@bottle.post( "/oauth/token/request" )
@bottle.route( "/oauth/token/request" )
def oauthRequestToken():
  oaRequest = getOARequest()

  result = checkRequest( oaRequest )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]
  tokenPair = gOADataStore.generateTokenPair( oaData[ 'consumer' ], request = True )
  reqToken = oauth2.Token( tokenPair[0], tokenPair[1] )
  return reqToken.to_string()

@bottle.post( "/oauth/token/authorize" )
@bottle.route( "/oauth/token/authorize" )
def oauthAuthorizeToken():
  oaRequest = getOARequest()

  result = checkRequest( oaRequest, checkRequestToken = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  return gOADataStore.generateRequestVerifier( oaData[ 'consumer' ], oaData[ 'request' ] )

@bottle.post( "/oauth/token/access" )
@bottle.route( "/oauth/token/access" )
def oauthAccessToken():
  oaRequest = getOARequest()

  result = checkRequest( oaRequest, checkRequestToken = True, checkVerifier = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  tokenPair = gOADataStore.generateTokenPair( oaData[ 'consumer' ], request = False )
  reqToken = oauth2.Token( tokenPair[0], tokenPair[1] )
  return reqToken.to_string()

@bottle.post( "/oauth/rawRequest" )
@bottle.route( "/oauth/rawRequest" )
def oauthRawRequest():
  oaRequest = getOARequest()

  result = checkRequest( oaRequest, checkAccessToken = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  if not gOADataStore.checkAccessToken( oaData[ 'consumer' ], oaData[ 'access' ] ):
    gLogger.info( "Not authorized access token" )
    bottle.abort( 401, "Invalid token" )

  #TODO: DO STUFF
  return "STUFF"


@bottle.route( "/oauth/echo" )
@bottle.post( "/oauth/echo" )
def echo():
  return bottle.request.query_string

