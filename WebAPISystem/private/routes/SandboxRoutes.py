import bottle
import tempfile
import os
import shutil
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData
from WebAPIDIRAC.WebAPISystem.private.Clients import getRPCClient, getTransferClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient

#GET    SELET
#POST   INSERT
#PUT    UPDATE
#DELETE DELETE

def uploadSandbox( fileList ):
  tmpDir = tempfile.mkdtemp( "IROK." )
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

  shutil.rmtree( tmpDir )
  return result


@bottle.route( "/sandbox/input", method = 'POST' )
def sendISB():
  reqFiles = bottle.request.files
  gLogger.info( "Received %s files for sandboxing" % len( reqFiles ) )
  result = self.uploadSandbox( reqFiles )

  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    bottle.abort( 500, result[ 'Message' ] )

  return { 'sandbox' : result[ 'Value' ] }

@bottle.route( "/sandbox/:type/:id", method = 'GET' )
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


