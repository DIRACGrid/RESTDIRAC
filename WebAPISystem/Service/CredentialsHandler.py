import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB

class CredentialsHandler( RequestHandler ):

  @classmethod
  def initialize( cls, serviceInfoDict ):
    cls.__credDB = CredentialsDB()
    return S_OK()

  auth_generateToken = [ Properties.TRUSTED_HOST ]
  types_generateToken = ( types.StringType, types.StringType,
                          types.StringType, ( types.IntType, types.LongType ) )
  def export_generateToken( self, userDN, userGroup, consumerKey, lifeTime ):
    return self.__credDB.generateToken( userDN, userGroup, consumerKey, lifeTime )

  auth_getTokenSecret = [ Properties.TRUSTED_HOST ]
  types_getTokenSecret = ( types.StringType, types.StringType,
                      types.StringType, types.StringType )
  def export_getTokenSecret( self, userDN, userGroup, consumerKey, token ):
    return self.__credDB.getTokenSecret( userDN, userGroup, consumerKey, token )

  auth_revokeUserToken = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_revokeUserToken = [ types.StringType, types.StringType, types.StringType ]
  def export_revokeUserToken( self, userDN, userGroup, token ):
    return self.__credDB.revokeUserToken( userDN, userGroup, token )

  auth_revokeToken = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR, 'all' ]
  types_revokeToken = [ types.StringType ]
  def export_revokeToken( self, token ):
    credDict = self.srv_getRemoteCredentials()
    for prop in ( Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ):
      if prop in credDict[ 'properties' ]:
        return self.__credDB.revokeToken( token )
    return self.__credDB.revokeToken( credDict[ 'DN' ], credDict[ 'group' ], token )

  auth_cleanExpired = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_cleanExpired = []
  def export_cleanExpired( self, minLifeTime = 0 ):
    try:
      minLifeTime = max( 0, int( minLifeTime ) )
    except ValueError:
      return S_ERROR( "Minimun life time has to be an integer " )
    return self.__credDB.cleanExpired( minLifeTime )

  auth_getTokens = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_getTokens = [ types.DictType ]
  def export_getTokens( self, condDict ):
    return self.__credDB.getTokens( condDict )

  auth_generateVerifier = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_generateVerifier = ( types.StringType, types.StringType, types.StringType,
                             ( types.IntType, types.LongType ) )
  def export_generateVerifier( self, userDN, userGroup, consumerKey, lifeTime ):
    return self.__credDB.generateVerifier( userDN, userGroup, consumerKey, lifeTime )

  auth_validateVerifier = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_validateVerifier = ( types.StringType, types.StringType, types.StringType, types.StringType )
  def export_validateVerifier( self, userDN, userGroup, consumerKey, verifier ):
    return self.__credDB.validateVerifier( userDN, userGroup, consumerKey, verifier )

  auth_generateConsumerPair = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_generateConsumerPair = []
  def export_generateConsumerPair( self, consumerKey = "" ):
    return self.__credDB.generateConsumerPair( consumerKey )

  auth_getConsumerSecret = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_getConsumerSecret = [ types.StringType ]
  def export_getConsumerSecret( self, consumerKey ):
    return self.__credDB.getConsumerSecret( consumerKey )

  auth_deleteConsumer = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_deleteConsumer = [ types.StringType ]
  def export_deleteConsumer( self, consumerKey ):
    return self.__credDB.deleteConsumer( consumerKey )

  auth_getAllConsumers = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_getAllConsumers = []
  def export_getAllConsumers( self ):
    return self.__credDB.getAllConsumers()

