from DIRAC.Core.DISET.RPCClient import RPCClient

class CredentialsClient:

  def __init__( self, RPCFunctor = None ):
    if not RPCFunctor:
      self.__RPCFunctor = RPCClient
    else:
      self.__RPCFunctor = RPCFunctor

  def __getRPC( self ):
    return self.__RPCFunctor( "WebAPI/Credentials" )

  def generateToken( self, userDN, userGroup, consumerKey, tokenType, lifeTime = 86400 ):
    return self.__getRPC().generateToken( userDN, userGroup, consumerKey, tokenType, lifeTime )

  def getSecret( self, userDN, userGroup, consumerKey, token, tokenType ):
    return self.__getRPC().getSecret( userDN, userGroup, consumerKey, token, tokenType )

  def revokeUserToken( self, userDN, userGroup, token ):
    return self.__getRPC().revokeUserToken( userDN, userGroup, token )

  def revokeToken( self, token ):
    return self.__getRPC().revokeToken( token )

  def cleanExpiredTokens( self ):
    return self.__getRPC().cleanExpiredTokens()

