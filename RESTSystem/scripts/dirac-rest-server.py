
import sys
from DIRAC.Core.Base import Script
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


if __name__ == "__main__":

  localCfg = LocalConfiguration()

  localCfg.setConfigurationForWeb( "REST" )
  localCfg.addMandatoryEntry( "/DIRAC/Setup" )
  localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
  localCfg.addDefaultEntry( "LogLevel", "INFO" )
  localCfg.addDefaultEntry( "LogColor", True )

  resultDict = localCfg.loadUserData()
  if not resultDict[ 'OK' ]:
    gLogger.initialize( serverName, "/" )
    gLogger.error( "There were errors when loading configuration", resultDict[ 'Message' ] )
    sys.exit( 1 )

  from RESTDIRAC.ConfigurationSystem.Client.Helpers import RESTConf
  from RESTDIRAC.RESTSystem.private.RESTApp import RESTApp

  result = RESTConf.isOK()
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit(1)

  restApp = RESTApp()

  result = restApp.bootstrap()
  if not result[ 'OK' ]:
    gLogger.fatal( result[ 'Message' ] )
    sys.exit(1)

  restApp.run()

