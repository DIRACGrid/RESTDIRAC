
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig

gBaseSection = "/RESTAPI"

def getOption( path, defaultValue = "" ):
  return gConfig.getValue( "%s/%s" % ( gBaseSection, path ), defaultValue )

def getCodeAuthURL():
  return getOption( "CodeAuthURL" )

def isOK():
  for option in ( "CodeAuthURL", ):
    if not getOption( option ):
      return S_ERROR( "Missing %s/%s option" % ( gBaseSection, option ) )
  return S_OK()
