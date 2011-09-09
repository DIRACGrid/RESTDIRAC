
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Security import X509Certificate
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

def getUserDN( environ ):
  userDN = False
  if 'HTTPS' not in environ or environ[ 'HTTPS' ] != 'on':
    gLogger.info( "Getting the DN from /OAuth/DebugDN" )
    userDN = gConfig.getValue( "/OAuth/DebugDN", "/yo" )
  elif 'SSL_CLIENT_S_DN' in environ:
    userDN = environ[ 'SSL_CLIENT_S_DN' ]
  elif 'SSL_CLIENT_CERT' in environ:
    userCert = X509Certificate.X509Certificate()
    result = userCert.loadFromString( environ[ 'SSL_CLIENT_CERT' ] )
    if not result[ 'OK' ]:
      errMsg = "Could not load SSL_CLIENT_CERT: %s" % result[ 'Message' ]
      gLogger.error( errMsg )
      return S_ERROR( errMsg )
    else:
      userDN = userCert.getSubjectDN()[ 'Value' ]
  else:
    errMsg = "Web server is not properly configured to get SSL_CLIENT_S_DN or SSL_CLIENT_CERT in env"
    gLogger.fatal( errMsg )
    return S_ERROR( errMsg )

  result = Registry.getUsernameForDN( userDN )
  if not result[ 'OK' ]:
    gLogger.info( "Could not get username for DN %s: %s" % ( userDN, result[ 'Message' ] ) )
    return result
  userName = result[ 'Value' ]
  gLogger.info( "Got username for user" " => %s for %s" % ( userName, userDN ) )
  return S_OK( ( userDN, userName ) )
