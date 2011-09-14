import bottle
import oauth2
import urlparse
import urllib
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAManager, gOAData
import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS

bottle.app().catchall = False

#Oauth flow
@bottle.post( "/oauth/request_token" )
@bottle.route( "/oauth/request_token" )
def oauthRequestToken():

  result = gOAManager.authorizeFlow()
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )

  callback = gOAData.callback
  if callback == 'oob':
    callback = ""

  result = gOAManager.credentials.generateRequest( gOAData.consumerKey, callback )
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  tokenData = result[ 'Value' ]
  reqToken = oauth2.Token( tokenData[ 'request' ], tokenData[ 'secret' ] )
  reqToken.set_callback( tokenData[ 'callback' ] )

  return reqToken.to_string()

@bottle.post( "/oauth/authorize" )
@bottle.route( "/oauth/authorize" )
def oauthAuthorizeToken():
  gOAManager.parse()

  if gOAData.token:
    webURL = WebAPICS.getAuthorizeURL()
    if not webURL:
      gLogger.fatal( "Missing WebURL location!" )
      bottle.abort( 500 )
    webURL = "%s?oauth_token=%s" % ( webURL, urllib.quote_plus( gOAData.token ) )
    if gOAData.callback:
      webURL = "%s&oauth_callback=%s" % ( webURL, urllib.quote_plus( gOAData.callback ) )
    gLogger.notice( "redirecting to %s" % webURL )
    bottle.redirect( webURL )

  bottle.abort( 400, "Missing request token" )

@bottle.post( "/oauth/access_token" )
@bottle.route( "/oauth/access_token" )
def oauthAccessToken():

  result = gOAManager.authorizeFlow( checkRequest = True, checkVerifier = True )
  if not result[ 'OK' ]:
    gLogger.info( "Not authorized request: %s" % result[ 'Message' ] )
    bottle.abort( 401, "Not authorized: %s" % result[ 'Message' ] )
  oaData = result[ 'Value' ]

  result = gOAManager.credentials.generateToken( gOAData.consumerKey,
                                                  gOAData.requestToken,
                                                  gOAData.verifier )
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
#@gOAManager.authorize
def echo():

  data = "PARAMS : %s\n" % urllib.urlencode( bottle.request.params )
  data += "QUERY_STRING: %s\n" % bottle.request.query_string

  return data

