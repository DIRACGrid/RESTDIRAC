""" Handler for managing jobs
"""

import types
import os
import json
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, TmpDir, RESTHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from diraccfg import CFG
from DIRAC.Core.Utilities.JDL import dumpCFGAsJDL

__RCSID__ = "$Id$"

class JobHandler( RESTHandler ):

  ROUTE = "/jobs(?:/([0-9]+))?"

  ATTRIBUTES = [ ( 'status', 'Status' ),
               ( 'minorStatus', 'MinorStatus' ),
               ( 'appStatus', 'ApplicationStatus' ),
               ( 'jid', 'JobID' ),
               ( 'reschedules', 'ReschefuleCounter' ),
               ( 'cpuTime', 'CPUTime' ),
               ( 'jobGroup', 'JobGroup' ),
               ( 'name', 'JobName' ),
               ( 'site', 'Site' ),
               ( 'setup', 'DIRACSetup' ),
               ( 'priority', 'UserPriority' ),
               ( 'ownerDN', 'ownerDN' ),
               ( 'ownerGroup', 'OwnerGroup' ),
               ( 'owner', 'Owner' ) ]

  NUMERICAL = ( 'jid', 'cpuTime', 'priority' )

  FLAGS = [ ( 'verified', 'VerifiedFlag' ),
            ( 'retrieved', 'RetrievedFlag' ),
            ( 'accounted', 'AccountedFlag' ),
            ( 'outputSandboxReady', 'OSandboxReadyFlag' ),
            ( 'inputSandboxReady', 'ISandboxReadyFlag' ),
            ( 'deleted', 'DeletedFlag' ),
            ( 'killed', 'KilledFlag' ) ]

  TIMES = [ ( 'lastSOL', 'LastSignOfLife' ),
            ( 'startExecution', 'StartExecTime' ),
            ( 'submission', 'SubmissionTime' ),
            ( 'reschedule', 'RescheduleTime' ),
            ( 'lastUpdate', 'LastUpdateTime' ),
            ( 'heartBeat', 'HeartBeatTime' ),
            ( 'endExecution' , 'EndExecTime' ) ]

  def __findIndexes( self, paramNames ):
    indexes = {}
    for k, convList in ( ( 'attrs', self.ATTRIBUTES ), ( 'flags', self.FLAGS ), ( 'times', self.TIMES ) ):
      indexes[ k ] = {}
      for attrPair in convList:
        try:
          iP = paramNames.index( attrPair[1] )
        except ValueError:
          #Not found
          pass
        indexes[ k ][ attrPair[0] ] = iP
    return indexes


  def _getJobs( self, selDict, startJob = 0, maxJobs = 500 ):
    result = RPCClient( "WorkloadManagement/JobMonitoring" ).getJobPageSummaryWeb( selDict,
                                                                                   [( 'JobID', 'DESC' )],
                                                                                   startJob, maxJobs, True )
    if not result[ 'OK' ]:
      return WErr( 500, result[ 'Message' ] )
    origData = result[ 'Value' ]
    totalRecords = origData[ 'TotalRecords' ]
    retData = { 'entries' : totalRecords, 'jobs' : [] }
    if totalRecords == 0:
      return WOK( retData )
    indexes = self.__findIndexes( origData[ 'ParameterNames' ] )
    records = origData[ 'Records' ]

    try:
      for record in records:
        job = {}
        for param in indexes[ 'attrs' ]:
          job[ param ] = record[ indexes[ 'attrs' ][ param ] ]
          if param in self.NUMERICAL:
            job[ param ] = int( float( job[ param ] ) )
        for k in ( 'flags', 'times' ):
          job[ k ] = {}
          for field in indexes[ k ]:
            value = record[ indexes[ k ][ field ] ]
            if str(value).lower() == "none":
              continue
            if k == 'flags':
              job[ k ][ field ] = value.lower() == 'true'
            else:
              job[ k ][ field ] = str(value)
        retData[ 'jobs' ].append( job )

    except Exception as exc:
      raise WErr( 403, reason = repr(exc) )

    return WOK( retData )

  @web.asynchronous
  @gen.engine
  def get( self, jid ):
    startJob = 0
    maxJobs = 100
    if jid:
      selDict = { 'JobID' : int( jid ) }
    else:
      selDict = {}
      for convList in ( self.ATTRIBUTES, self.FLAGS ):
        for attrPair in convList:
          jAtt = attrPair[0]
          if jAtt in self.request.arguments:
            selDict[ attrPair[1] ] = self.request.arguments[ jAtt ]
      args = self.request.arguments
      if 'allOwners' not in args:
        selDict[ 'Owner' ] = self.getUserName()
      if 'startJob' in args:
        try:
          startJob = max( startJob, int( args[ 'startJob' ][-1] ) )
        except ValueError:
          raise WErr( 400, reason = "startJob has to be an integer" )
      if 'maxJobs' in args:
        try:
          maxJobs = max( maxJobs, int( args[ 'maxJobs' ][-1] ) )
        except ValueError:
          raise WErr( 400, reason = "maxJobs has to be an integer" )

    result = yield self.threadTask( self._getJobs, selDict, startJob, maxJobs )
    if not result.ok:
      raise result
    data = result.data
    if not jid:
      self.finish( data )
      return
    if data[ 'entries' ] == 0:
      raise WErr( 404, "Unknown jid" )
    self.finish( data[ 'jobs' ][0] )

  #POST A JOB :)

  def uploadSandbox( self, fileData ):
    with TmpDir() as tmpDir:
      fileList = []
      for fName in fileData:
        for entry in fileData[ fName ]:
          tmpFile = os.path.join( tmpDir, entry.filename )
          if tmpFile not in fileList:
            fileList.append( tmpFile )
          dfd = open( tmpFile, "w" )
          dfd.write( entry.body )
          dfd.close()
      sbClient = SandboxStoreClient()
      result = sbClient.uploadFilesAsSandbox( fileList )
      if not result[ 'OK' ]:
        return WErr( 500, result[ 'Message' ] )
      return WOK( result[ 'Value' ] )

  @web.asynchronous
  @gen.engine
  def post( self, jid ):
    if jid:
      self.send_error( 404 )
      return
    args = self.request.arguments
    if 'manifest' not in args:
      raise WErr( 400, "No manifest" )
    manifests = []
    for manifest in args[ 'manifest' ]:
      try:
        manifest = json.loads( manifest )
      except ValueError:
        raise WErr( 400, "Manifest is not JSON" )
      if type( manifest ) != types.DictType:
        raise WErr( 400, "Manifest is not an associative array" )
      manifests.append( manifest )

    #Upload sandbox
    files = self.request.files
    if files:
      result = yield self.threadTask( self.uploadSandbox, files )
      if not result.ok:
        self.log.error( "Cannot upload sandbox: %s" % result.msg )
        raise result
      sb = result.data
      self.log.info( "Uploaded to %s" % sb )
      for manifest in manifests:
        isb = manifest.get( 'InputSandbox', [] )
        if type( isb ) != types.ListType:
          isb = [ isd ]
        isb.append( sb )
        manifest[ 'InputSandbox' ] = isb

    #Send jobs
    jids = []
    rpc = RPCClient( 'WorkloadManagement/JobManager' )
    for manifest in manifests:
      jdl = dumpCFGAsJDL( CFG().loadFromDict( manifest ) )
      result = yield self.threadTask( rpc.submitJob, str( jdl ) )
      if not result[ 'OK' ]:
        self.log.error( "Could not submit job: %s" % result[ 'Message' ] )
        raise WErr( 500, result[ 'Message' ] )
      data = result[ 'Value' ]
      if type( data ) == types.ListType:
        jids.extend( data )
      else:
        jids.append( data )
    self.log.info( "Got jids %s" % jids )

    self.finish( { 'jids' : jids } )

  #KILL A JOB
  @web.asynchronous
  @gen.engine
  def delete( self, jid ):
    if not jid:
      self.send_error( 404 )
      return
    try:
      jid = int( jid )
    except ValueError:
      raise WErr( 400, "Invalid jid" )
    rpc = RPCClient( 'WorkloadManagement/JobManager' )
    args = self.request.arguments
    if 'killonly' in args and args[ 'killonly' ]:
      result = yield self.threadTask(  rpc.killJob, [ jid ]  )
    else:
      result = yield self.threadTask(  rpc.deleteJob, [ jid ]  )
    if not result[ 'OK' ]:
      if 'NonauthorizedJobIDs' in result:
        #Not authorized
        raise WErr( 401, "Not authorized" )
      if 'InvalidJobIDs' in result:
        #Invalid jid
        raise WErr( 400, "Invalid jid" )
      if 'FailedJobIDs' in result:
        # "Could not delete JID"
        raise WErr( 500, "Could not delete" )
    self.finish( { 'jid' : jid } )
