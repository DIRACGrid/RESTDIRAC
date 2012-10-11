
import hashlib
import datetime
from tornado import web, gen
from RESTDIRAC.RESTSystem.API.RESTHandler import WErr, WOK, TmpDir, RESTHandler
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL
from DIRAC.Core.Utilities import List, CFG

class JobStuffHandler( RESTHandler ):

  ROUTE = "/jobs/([0-9]+)/([a-z]+)"

  def _getJobManifest( self, jid ):
    result = RPCClient( "WorkloadManagement/JobMonitoring" ).getJobJDL( int( jid  ) )
    if not result[ 'OK' ]:
      return WErr( 500, result[ 'Message' ] )
    result = loadJDLAsCFG( result[ 'Value' ] )
    if not result[ 'OK' ]:
      return WErr( 500, result[ 'Message' ] )
    cfg = result[ 'Value' ][0]
    jobData = {}
    stack = [ ( cfg, jobData ) ]
    while stack:
      cfg, level = stack.pop( 0 )
      for op in cfg.listOptions():
        val = List.fromChar( cfg[ op ] )
        if len( val ) == 1:
          val = val[0]
        level[ op ] = val
      for sec in cfg.listSections():
        level[ sec ] = {}
        stack.append( ( cfg[ sec ], level[ sec ] ) )
    return WOK( jobData )


  def _getJobSB( self, jid, objName ):
    with TmpDir() as tmpDir:
      if objName == "outputsandbox":
        objName = "Output"
      else:
        objName = "Input"
      result = SandboxStoreClient().downloadSandboxForJob( int( jid ), objName, tmpDir, inMemory = True )
      if not result[ 'OK' ]:
        msg = result[ 'Message' ]
        if msg.find( "No %s sandbox" % objName ) == 0:
          return WErr( 404, "No %s sandbox defined for job %s" % ( jid, objName.lower() ) )
        return WErr( 500, result[ 'Message' ] )
      return WOK( result[ 'Value' ] )

  @web.asynchronous
  @gen.engine
  def get( self, jid, objName ):
    if objName == "description":
      result = yield self.threadTask( self._getJobManifest, jid )
      if not result.ok:
        self.log.error( result.msg )
        raise result
      self.finish( result.data )
    elif objName in ( "outputsandbox", "inputsandbox" ):
      result = yield self.threadTask( self._getJobSB, jid, objName )
      if not result.ok:
        self.log.error( result.msg )
        raise result
      data = result.data
      self.clear()
      self.set_header( "Content-Type", "application/x-tar" )
      cacheTime = 86400
      self.set_header( "Expires", datetime.datetime.utcnow() + datetime.timedelta( seconds = cacheTime ) )
      self.set_header( "Cache-Control", "max-age=%d" % cacheTime )
      self.set_header( "ETag", '"%s"' % hashlib.sha1( data ).hexdigest )
      self.set_header( "Content-Disposition", 'attachment; filename="%s-%s.tar.gz"' % ( jid, objName ) )
      self.finish( data )
    else:
      raise WErr( 404, "Invalid job object" )

