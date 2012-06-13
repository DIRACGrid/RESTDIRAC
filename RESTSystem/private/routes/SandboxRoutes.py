import bottle
import tempfile
import os
import urllib
import shutil
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData
from WebAPIDIRAC.WebAPISystem.private.Clients import getRPCClient, getTransferClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.Core.Utilities import DictCache
import WebAPIDIRAC.ConfigurationSystem.Client.Helpers.WebAPI as WebAPICS

#GET    SELET
#POST   INSERT
#PUT    UPDATE
#DELETE DELETE

def deleteFile( filePath ):
  try:
    os.unlink( filePath )
  except:
    pass

gFileCache = DictCache( deleteFile )
gWorkDir = WebAPICS.getWorkDir()

def initialize():
  if not os.path.isdir( gWorkDir ):
    try:
      os.makedirs( gWorkDir )
    except:
      raise RuntimeError( "Can't create %s" % gWorkDir )
  for ent in os.listdir( gWorkDir ):
    ent = os.path.join( gWorkDir, ent )
    if os.path.isdir( ent ):
      shutil.rmtree( ent )
    else:
      os.unlink( ent )


def uploadSandbox( fileList ):
  tmpDir = tempfile.mkdtemp( prefix = "upload.", dir = gWorkDir )
  fileList = []
  for fileName in fileList:
    fDir = os.path.dirname( fileName )
    if fDir:
      fDir = os.path.join( tmpDir, fDir )
      if not os.path.isdir( fDir ):
        try:
          os.makedirs( fDir )
        except:
          gLogger.exception( "Could not create temporal dir %s" % fDir )
          bottle.abort( 500 )
      absFile = os.path.join( tmpDir, fileName )
      fileList.append( absFile )
      ofd = reqFiles[ fileName ]
      dfd = open( absFile, "w" )
      dBuf = ofd.read( 524288 )
      while dBug:
        dfd.write( dBuf )
        dBuf = ofd.read( 524288 )
      dfd.close()
      dBuf.close()
  sbClient = SandboxStoreClient( useCertificates = True, delegatedDN = gOAData.userDN,
                                 delegatedGroup = gOAData.userGroup )
  result = sbClient.uploadFilesAsSandbox( fileList )

  if result[ 'OK' ]:
    sburl = result[ 'Value' ]
    result = S_OK( { 'sandbox' : sburl, 'urlsafe' : urllib.quote( sburl, safe = '~' ) } )

  shutil.rmtree( tmpDir )
  return result


@bottle.route( "/sandbox/input", method = 'POST' )
def sendISB():
  reqFiles = bottle.request.files
  gLogger.info( "Received %s files for sandboxing" % len( reqFiles ) )
  result = uploadSandbox( reqFiles )

  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    bottle.abort( 500, result[ 'Message' ] )

  return result

@bottle.route( "/sandbox/list/:type/:id", method = 'GET' )
def listSandboxes( type, id ):
  type = type.lower()
  if type not in ( 'job', 'pilot' ):
    bottle.abort( 404 )
  try:
    id = int( id )
  except ValueError:
    bottle.abort( 400, "id has to be an integer" )
  sbClient = SandboxStoreClient( useCertificates = True, delegatedDN = gOAData.userDN,
                                 delegatedGroup = gOAData.userGroup )
  if type == "job":
    result = sbClient.getSandboxesForJob( id )
  else:
    result = sbClient.getSandboxesForPilot( id )

  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  return result[ 'Value' ]

@bottle.route( "/sandbox" )
def getSandbox():
  request = bottle.request
  if 'sburl' not in request.query:
    bottle.abort( 400, "Missing sburl parameter" )
  sburl = urllib.unquote( request.query[ 'sburl' ] )

  sbClient = SandboxStoreClient( useCertificates = True, delegatedDN = gOAData.userDN,
                                 delegatedGroup = gOAData.userGroup )
  tmpDir = tempfile.mkdtemp( prefix = "down.", dir = gWorkDir )
  result = sbClient.downloadSandbox( sburl, tmpDir, unpack = False )
  if not result[ 'OK' ]:
    print result
    os.rmdir( tmpDir )
    bottle.abort( 401, "Can't download %s" % sburl )
  print result



