
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig

gBaseSection = "/WebAPI"

def getOption( path, defaultValue = "" ):
  return gConfig.getValue( "%s/%s" % ( gBaseSection, path ), defaultValue )

def getDIRACWebURL():
  return getOption( "WebURL" )

def getAuthorizeURL():
  url = getOption( "WebURL" )
  if url:
    authPath = getOption( "AuthorizationURLPath", "/WebAPI/authorizeRequest" )
    while authPath[0] == "/":
      authPath = authPath[1:]
    while url[-1] == "/":
      url = url[:-1]
    return "%s/%s" % ( url, authPath )
  return False

def useLocalDB():
  return getOption( "LocalDB", False )

def isOK():
  for option in ( "WebURL", ):
    if not getOption( option ):
      return S_ERROR( "Missing %s/%s option" % ( gBaseSection, option ) )
  return S_OK()
