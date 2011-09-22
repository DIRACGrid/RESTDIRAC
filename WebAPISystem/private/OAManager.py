
import urlparse
import oauth2
import threading

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from WebAPIDIRAC.ConfigurationSystem.Client.Helpers import Registry
from WebAPIDIRAC.WebAPISystem.Client.CredentialsWrapper import getCredentialsClient


class OAData( threading.local ):

  def __init__( self ):
    self.__oaData = {}
    self.__tokenData = {}
    self.__cred = getCredentialsClient()

  def bind( self, oaData ):
    self.__oaData = oaData
    if self.__tokenData:
      self.__tokenData = {}
    if 'access_token' in oaData:
      result = self.__cred.getTokenData( oaData[ 'consumer_key' ], oaData[ 'access_token' ] )
      if result[ 'OK' ]:
        self.__tokenData = result[ 'Value' ]

  def __inOAData ( self, key ):
    if key in self.__oaData:
      return str( self.__oaData[ key ] )
    return False

  def __inTokenData( self, key ):
    if key in self.__tokenData:
      return str( self.__tokenData[ key ] )
    return False

  @property
  def userDN( self ):
    return self.__inTokenData( 'userDN' )

  @property
  def userName( self ):
    DN = self.__inTokenData( 'userDN' )
    if not DN:
      return False
    result = Registry.getUsernameForDN( DN )
    if not result[ 'OK' ]:
      return False
    return result[ 'Value' ]


  @property
  def userGroup( self ):
    return self.__inTokenData( 'userGroup' )


  @property
  def consumerKey( self ):
    return self.__inOAData( 'consumer_key' )

  @property
  def consumerSecret( self ):
    return self.__inOAData( 'consumer_secret' )

  @property
  def token( self ):
    return self.__inOAData( 'token' )

  @property
  def tokenSecret( self ):
    return self.__inOAData( 'token_secret' )

  @property
  def requestToken( self ):
    return self.__inOAData( 'request_token' )

  @property
  def requestSecret( self ):
    return self.__inOAData( 'request_secret' )

  @property
  def accessToken( self ):
    return self.__inOAData( 'access_token' )

  @property
  def accessSecret( self ):
    return self.__inOAData( 'access_secret' )

  @property
  def verifier( self ):
    return self.__inOAData( 'verifier' )

  @property
  def callback( self ):
    return self.__inOAData( 'callback' )



class OAManager( object ):

  def __init__( self ):
    self.__cred = getCredentialsClient()
    self.__oaServer = oauth2.Server()
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_HMAC_SHA1() )
    self.__oaServer.add_signature_method( oauth2.SignatureMethod_PLAINTEXT() )

  @property
  def credentials( self ):
    return self.__cred

  def authorizeFlow( self, oaRequest = False, checkRequest = False, checkToken = False, checkVerifier = False ):
    if not oaRequest:
      oaRequest = self.parse()
    if 'oauth_consumer_key' not in oaRequest:
      return S_ERROR( "No consumer key in request" )
    consumerKey = oaRequest[ 'oauth_consumer_key' ]
    result = self.__cred.getConsumerData( str( consumerKey ) )
    if not result[ 'OK' ]:
      return result
    expectedSecret = result[ 'Value' ][ 'secret' ]
    oaConsumer = oauth2.Consumer( consumerKey, expectedSecret )

    oaData = {}
    oaData[ 'consumer_key' ] = consumerKey
    oaData[ 'consumer_secret ' ] = expectedSecret

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
        tokenType = 'access'
        result = self.__cred.getTokenData( consumerKey, tokenString )
        if not result[ 'OK' ]:
          return result
        expectedSecret = result[ 'Value' ][ 'secret' ]
      oaToken = oauth2.Token( tokenString, expectedSecret )
      oaData[ "%s_token" % tokenType ] = tokenString
      oaData[ "token" ] = tokenString
      oaData[ "secret" ] = expectedSecret

    try:
      self.__oaServer.verify_request( oaRequest, oaConsumer, oaToken )
    except oauth2.Error, e:
      return S_ERROR( "Invalid request: %s" % e )

    for key in ( 'verifier', 'callback' ):
      oakey = "oauth_%s" % key
      if oakey in oaRequest:
        oaData[ key ] = oaRequest[ oakey ]

    if checkVerifier:
      if 'oauth_verifier' not in oaRequest:
        return S_ERROR( "No verifier provided by consumer" )

    gOAData.bind( oaData )

    return S_OK( gOAData )

  def parse( self, method, url, headers, parameters, query_string ):

    oaRequest = oauth2.Request.from_request( method,
                                             url,
                                             headers,
                                             parameters,
                                             query_string )
    oaData = {}
    for key in ( 'consumer_key', 'callback', 'verifier', 'token' ):
      oakey = "oauth_%s" % key
      if oakey in oaRequest:
        oaData[ key ] = oaRequest[ oakey ]
    gOAData.bind( oaData )

    return oaRequest

  def notAuthorized( self ):
    raise RuntimeError( "Not authorized" )

  def authorize( self, funct ):
    oaRequest = self.parse()
    result = self.authorizeFlow( oaRequest, checkToken = True )
    if not result[ 'OK' ]:
      return self.notAuthorized
    gLogger.notice( "Delegated by %s@%s" % ( gOAData.userDN, gOAData.userGroup ) )
    return funct


gOAManager = OAManager()
gOAData = OAData()
