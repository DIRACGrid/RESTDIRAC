
import bottle
import urlparse
import oauth2

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Security import X509Certificate
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient

class OAuthHelper():


  def __init__( self ):
    self.__cred = CredentialsClient()
    self.__oaServer = oauth2.Server()
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_HMAC_SHA1() )
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_PLAINTEXT() )


  def getConsumerSecret( self, consumerKey ):
    return self.__cred.getConsumerSecret( consumerKey )

  def generateRequest( self, consumerKey ):
    return self.__cred.generateRequesT( consumerKey )

  def generateVerifier( self, consumerKey, request ):
    return self.__cred.generateVerifier( consumerKey, request )

  def generateToken( self, consumerKey, request, verifier, lifeTime = 86400 ):
    return self.__cred.generateToken( consumerKey, request, verifier, lifeTim )

  def getTokenSecret( self, consumerKey, request ):
    return self.__cred.getRequestSecret( consumerKey, request )


  def checkRequest( self, oaRequest, checkRequest = False, checkToken = False, checkVerifier = False ):
    consumerKey = oaRequest[ 'oauth_consumer_key' ]
    expectedSecret = self.getConsumerSecret( consumerKey )
    if not expectedSecret:
      return S_ERROR( "Unknown consumer key" )
    oaConsumer = oauth2.Consumer( consumerKey, expectedSecret )

    oaData = {}
    oaData[ 'consumer' ] = consumerKey

    oaToken = False

    if checkRequest or checkToken:
      if 'oauth_token' not in oaRequest:
        return S_ERROR( "No token in request " )
      tokenString = oaRequest[ 'oauth_token' ]
      if checkRequest:
        type = 'request'
        result = self.__cred.getRequestSecret( consumerKey, tokenString )
        if not result[ 'OK' ]:
          return result
        expectedSecret = result[ 'Value' ]
      else:
        type = 'token'
        result = self.__cred.getTokenData( consumerKey, tokenString )
        if not result[ 'OK' ]:
          return result
        expectedSecret = result[ 'Value' ][ 'secret' ]
      oaToken = oauth2.Token( tokenString, expectedSecret )
      oaData[ type ] = tokenString

    try:
      self.__oaServer.verify_request( oaRequest, oaConsumer, oaToken )
    except oauth2.Error, e:
      return S_ERROR( "Invalid request: %s" % e )

    if checkVerifier:
      if oaRequest['oauth_verifier'] != self.__cred.getVerifier( consumerToken, reqToken ):
        return S_ERROR( "Invalid verifier" )
      oaData[ 'verify' ] = oaRequest['oauth_verifier']

    return S_OK( oaData )

