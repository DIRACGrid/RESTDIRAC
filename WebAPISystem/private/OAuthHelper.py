
import bottle
import urlparse
import oauth2

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Security import X509Certificate
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from WebAPIDIRAC.WebAPISystem.Client.CredentialsWrapper import getCredentialsClient

class OAuthHelper():


  def __init__( self ):
    self.__cred = getCredentialsClient( local = True )
    self.__oaServer = oauth2.Server()
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_HMAC_SHA1() )
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_PLAINTEXT() )

  def getWebAuthorizationURL( self ):
    baseURL = "http://localhost:5001"
    return "%s/WebAPI/authorizeRequest" % baseURL


  def getConsumerSecret( self, consumerKey ):
    result = self.__cred.getConsumerData( consumerKey )
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'Value' ][ 'secret' ] )

  def getConsumerData( self, consumerKey ):
    return self.__cred.getConsumerData( consumerKey )

  def generateRequest( self, consumerKey, callback = "" ):
    return self.__cred.generateRequest( consumerKey, callback )

  def getRequestData( self, reqToken ):
    return self.__cred.getRequestData( reqToken )

  def generateVerifier( self, userDN, userGroup, consumerKey, request ):
    return self.__cred.generateVerifier( userDN, userGroup, consumerKey, request )

  def setVerifierProperties( self, consumerKey, request, verifier, userDN, userGroup, lifeTime ):
    return self.__cred.setVerifierProperties( consumerKey, request, verifier, userDN, userGroup, lifeTime )

  def generateToken( self, consumerKey, request, verifier ):
    return self.__cred.generateToken( consumerKey, request, verifier )

  def getTokenSecret( self, consumerKey, request ):
    return self.__cred.getRequestSecret( consumerKey, request )


  def checkRequest( self, oaRequest, checkRequest = False, checkToken = False, checkVerifier = False ):
    if 'oauth_consumer_key' not in oaRequest:
      return S_ERROR( "No consumer key in request" )
    consumerKey = oaRequest[ 'oauth_consumer_key' ]
    result = self.getConsumerSecret( consumerKey )
    if not result[ 'OK' ]:
      return result
    expectedSecret = result[ 'Value' ]
    oaConsumer = oauth2.Consumer( consumerKey, expectedSecret )

    oaData = {}
    oaData[ 'consumer' ] = consumerKey

    oaToken = False

    if checkRequest or checkToken:
      if 'oauth_token' not in oaRequest:
        return S_ERROR( "No token in request " )
      tokenString = oaRequest[ 'oauth_token' ]
      if checkRequest:
        tokenType = 'request'
        result = self.__cred.getRequestData( tokenString )
        if not result[ 'OK' ]:
          return result
        expectedSecret = result[ 'Value' ][ 'secret' ]
      else:
        tokenType = 'token'
        result = self.__cred.getTokenData( consumerKey, tokenString )
        if not result[ 'OK' ]:
          return result
        expectedSecret = result[ 'Value' ][ 'secret' ]
      oaToken = oauth2.Token( tokenString, expectedSecret )
      oaData[ tokenType ] = tokenString

    try:
      self.__oaServer.verify_request( oaRequest, oaConsumer, oaToken )
    except oauth2.Error, e:
      return S_ERROR( "Invalid request: %s" % e )

    if checkVerifier:
      if 'oauth_verifier' not in oaRequest:
        return S_ERROR( "No verifier provided by consumer" )
      oaData[ 'verifier' ] = oaRequest['oauth_verifier']

    if 'oauth_callback' in oaRequest:
      oaData[ 'callback' ] = oaRequest[ 'oauth_callback' ]

    return S_OK( oaData )

