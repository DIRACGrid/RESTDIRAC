import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from OAuthDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB

class CredentialsHandler( RequestHandler ):

  @classmethod
  def initialize( cls, serviceInfoDict ):
    cls.__credDB = CredentialsDB()
    return S_OK()

  auth_generateToken = [ Properties.TRUSTED_HOST ]
  types_generateToken = ( types.StringType, types.StringType, types.StringType,
                          types.StringType, ( types.IntType, types.LongType ) )
  def export_generateToken( self, userDN, userGroup, consumerKey, tokenType, lifeTime ):
    return self.__credDB.generateToken( userDN, userGroup, consumerKey, tokenType, lifeTime )

  auth_getSecret = [ Properties.TRUSTED_HOST ]
  types_getSecret = ( types.StringType, types.StringType, types.StringType,
                      types.StringType, types.StringType )
  def export_getSecret( self, userDN, userGroup, consumerKey, token, tokenType ):
    return self.__credDB.getSecret( userDN, userGroup, consumerKey, token, tokenType )

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

  auth_cleanExpiredTokens = [ Properties.TRUSTED_HOST, Properties.SERVICE_ADMINISTRATOR ]
  types_cleanExpiredTokens = []
  def export_cleanExpiredTokens( self ):
    return self.__credDB.cleanExpiredTokens()





