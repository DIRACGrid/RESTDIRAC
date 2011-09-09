from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.DictCache import DictCache

class CredentialsClient:

  CONSUMER_GRACE_TIME = 3600

  def __init__( self, RPCFunctor = None ):
    if not RPCFunctor:
      self.__RPCFunctor = RPCClient
    else:
      self.__RPCFunctor = RPCFunctor
    self.__tokens = DictCache()
    self.__consumers = DictCache()

  def __cleanReturn( self, result ):
    if 'rpcStub' in result:
      result.pop( 'rpcStub' )
    return result

  def __getRPC( self ):
    return self.__RPCFunctor( "WebAPI/Credentials" )

  def generateToken( self, userDN, userGroup, consumerKey, tokenType, lifeTime = 86400 ):
    result = self.__getRPC().generateToken( userDN, userGroup, consumerKey, tokenType, lifeTime )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    tokenPair = result[ 'Value' ]
    cKey = ( userDN, userGroup, consumerKey, tokenPair[0], tokenType.lower() )
    self.__tokens.add( cKey, lifeTime - 5, tokenPair[1] )
    return self.__cleanReturn( result )

  def getSecret( self, userDN, userGroup, consumerKey, token, tokenType ):
    cKey = ( userDN, userGroup, consumerKey, token, tokenType.lower() )
    secret = self.__tokens.get( cKey )
    if secret:
      return S_OK( secret )
    result = self.__getRPC().getSecret( userDN, userGroup, consumerKey, token, tokenType )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    self.__tokens.add( cKey, result[ 'lifeTime' ] - 5, result[ 'Value' ] )
    return self.__cleanReturn( result )

  def revokeUserToken( self, userDN, userGroup, token ):
    result = self.__getRPC().revokeUserToken( userDN, userGroup, token )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    cKeys = self.__tokens.getKeys()
    for cKey in cKeys:
      if cKey[0] == userDN and cKey[1] == userGroup and cKey[3] == token:
        self.__tokens.delete( cKey )
    return self.__cleanReturn( result )

  def revokeToken( self, token ):
    result = self.__getRPC().revokeToken( token )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    cKeys = self.__tokens.getKeys()
    for cKey in cKeys:
      if cKey[3] == token:
        self.__tokens.delete( cKey )
    return self.__cleanReturn( result )

  def cleanExpired( self ):
    return self.__getRPC().cleanExpired()

  def getTokens( self, condDict = {} ):
    result = self.__getRPC().getTokens( condDict )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    params = result[ 'Value' ][ 'Parameters']
    data = result[ 'Value' ][ 'Records' ]
    fieldOrder = []
    secretField = 1
    lifeField = -1
    for key in ( "UserDN", "UserGroup", "ConsumerKey", "Token", "Type" ):
      for iPos in range( len( params ) ):
        if params[ iPos ] == key:
          fieldOrder.append( iPos )
          break
    for iPos in range( len( params ) ):
      if params[ iPos ] == "Secret":
        secretField = 1
      elif params[ iPos ] == "LifeTime":
        lifeField = iPos

    for record in data:
      cKey = []
      for iPos in fieldOrder:
        cKey.append( record[iPos] )
      cKey[-1] = cKey[-1].lower()
      cKey = tuple( cKey )
      self.__tokens.add( cKey, record[ lifeField ] - 5, record[ secretField ] )

    return self.__cleanReturn( result )


  def generateVerifier( self, userDN, userGroup, consumerKey, lifeTime = 3600 ):
    result = self.__getRPC().generateVerifier( userDN, userGroup, consumerKey, lifeTime )
    return self.__cleanReturn( result )

  def validateVerifier( self, userDN, userGroup, consumerKey, verifier ):
    result = self.__getRPC().validateVerifier( userDN, userGroup, consumerKey, verifier )
    return self.__cleanReturn( result )

  def generateConsumerPair( self, consumerKey = "" ):
    result = self.__getRPC().generateConsumerPair( consumerKey )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    consumerKey, secret = result[ 'Value' ]
    self.__consumers.add( consumerKey, self.CONSUMER_GRACE_TIME, secret )
    return self.__cleanReturn( result )

  def getConsumerSecret( self, consumerKey ):
    secret = self.__consumers.get( consumerKey )
    if secret:
      return secret
    result = self.__getRPC().getConsumerSecret( consumerKey )
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    self.__consumers.add( consumerKey, self.CONSUMER_GRACE_TIME, result[ 'Value' ] )
    return self.__cleanReturn( result )

  def deleteConsumer( self, consumerKey ):
    self.__consumers.delete( consumerKey )
    result = self.__getRPC().deleteConsumer( consumerKey )
    if result[ 'OK' ]:
      cKeys = self.__tokens.getKeys()
      for cKey in cKeys:
        if cKey[2] == consumerKey:
          self.__tokens.delete( cKey )
    return self.__cleanReturn( result )

  def getAllConsumers( self ):
    result = self.__getRPC().getAllConsumers()
    if not result[ 'OK' ]:
      return self.__cleanReturn( result )
    for record in result[ 'Value' ]:
      self.__consumers.add( record[0], self.CONSUMER_GRACE_TIME, record[1] )
    return self.__cleanReturn( result )


