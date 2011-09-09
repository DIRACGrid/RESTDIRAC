
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

  def getTokenSecret( self, consumerKey, tokenString ):
    pass


  def checkRequest( self, oaRequest, checkRequestToken = False, checkAccessToken = False, checkVerifier = False ):
    consumerKey = oaRequest[ 'oauth_consumer_key' ]
    expectedSecret = self.getConsumerSecret( consumerKey )
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
        result = self.getRequestSecret( consumerKey, tokenString )
        checkRequest = True
        tokenType = "request"
      else:
        checkRequest = False
        tokenType = "access"
      expectedSecret = self.getTokenSecret( consumerToken, tokenString, checkRequest )
      if not expectedSecret:
        return S_ERROR( "Unknown %s token" % tokenType )
      oaToken = oauth2.Token( tokenString, expectedSecret )
      oaData[ tokenType ] = tokenString

    try:
      self.__oaServer.verify_request( oaRequest, oaConsumer, oaToken )
    except oauth2.Error, e:
      return S_ERROR( "Invalid request: %s" % e )

    if checkVerifier:
      if oaRequest['oauth_verifier'] != gOADataStore.getRequestVerifier( consumerToken, reqToken ):
        return S_ERROR( "Invalid verifier" )
      oaData[ 'verify' ] = oaRequest['oauth_verifier']

    return S_OK( oaData )

