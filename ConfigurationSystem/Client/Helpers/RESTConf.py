
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR, rootPath
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Security import Locations, X509Chain
import tempfile

gBaseSection = "/REST"

def getOption( path, defaultValue = "" ):
  return gConfig.getValue( "%s/%s" % ( gBaseSection, path ), defaultValue )

def getCodeAuthURL():
  return getOption( "CodeAuthURL" )

def getWorkDir():
  return getOption( "WorkDir", os.path.join( rootPath, "workDir", "REST" ) )

def debug():
  return getOption( "Debug", False )

def balancer():
  return getOption( "Balancer", "" )

def numProcesses():
  return getOption( "NumProcesses", -1 )

def port():
  return getOption( "Port", 9910 )

def cert():
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"
  return getOption( "HTTPS/Cert", cert )

def key():
  key = Locations.getHostCertificateAndKeyLocation()
  if key:
    key = key[1]
  else:
    key = "/opt/dirac/etc/grid-security/hostkey.pem"
  return getOption( "HTTPS/Key", key )

def setup():
  return gConfig.getValue( "/DIRAC/Setup" )

def generateCAFile():
  """
  Generate a single CA file with all the PEMs
  """
  caDir = Locations.getCAsLocation()
  for fn in ( os.path.join( os.path.dirname( caDir ), "cas.pem" ),
              os.path.join( os.path.dirname( cert() ), "cas.pem" ),
              False ):
    if not fn:
      fn = tempfile.mkstemp( prefix = "cas.", suffix = ".pem" )
    try:
      fd = open( fn, "w" )
    except IOError:
      continue
    for caFile in os.listdir( caDir ):
      caFile = os.path.join( caDir, caFile )
      result = X509Chain.X509Chain.instanceFromFile( caFile )
      if not result[ 'OK' ]:
        continue
      chain = result[ 'Value' ]
      expired = chain.hasExpired()
      if not expired[ 'OK' ] or expired[ 'Value' ]:
        continue
      fd.write( chain.dumpAllToString()[ 'Value' ] )
    fd.close()
    return fn
  return False


def isOK():
  for option in ( "CodeAuthURL", ):
    if not getOption( option ):
      return S_ERROR( "Missing %s/%s option" % ( gBaseSection, option ) )
  return S_OK()
