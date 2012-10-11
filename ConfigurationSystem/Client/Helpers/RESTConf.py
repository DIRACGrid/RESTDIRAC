
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR, rootPath
from DIRAC.ConfigurationSystem.Client.Config import gConfig

gBaseSection = "/RESTAPI"

def getOption( path, defaultValue = "" ):
  return gConfig.getValue( "%s/%s" % ( gBaseSection, path ), defaultValue )

def getCodeAuthURL():
  return getOption( "CodeAuthURL" )

def getWorkDir():
  return getOption( "WorkDir", os.path.join( rootPath, "workDir", "REST" ) )

def isOK():
  for option in ( "CodeAuthURL", ):
    if not getOption( option ):
      return S_ERROR( "Missing %s/%s option" % ( gBaseSection, option ) )
  return S_OK()
