import bottle
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData, gOAManager
from WebAPIDIRAC.WebAPISystem.private.Clients import getRPCClient, getTransferClient
from WebAPIDIRAC.WebAPISystem.private.routes.SandboxRoutes import uploadSandbox
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL
from DIRAC.Core.Utilities import List, CFG
import DIRAC.Core.Utilities.Time as Time
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient

#GET    SELET
#POST   INSERT
#PUT    UPDATE
#DELETE DELETE

attrConv = [ ( 'status', 'Status' ),
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

integerFields = ( 'jid', 'cpuTime', 'priority' )

flagConv = [ ( 'verified', 'VerifiedFlag' ),
             ( 'retrieved', 'RetrievedFlag' ),
             ( 'accounted', 'AccountedFlag' ),
             ( 'outputSandboxReady', 'OSandboxReadyFlag' ),
             ( 'inputSandboxReady', 'ISandboxReadyFlag' ),
             ( 'deleted', 'DeletedFlag' ),
             ( 'killed', 'KilledFlag' ) ]

timesConv = [ ( 'lastSOL', 'LastSignOfLife' ),
              ( 'startExecution', 'StartExecTime' ),
              ( 'submission', 'SubmissionTime' ),
              ( 'reschedule', 'RescheduleTime' ),
              ( 'lastUpdate', 'LastUpdateTime' ),
              ( 'heartBeat', 'HeartBeatTime' ),
              ( 'endExecution' , 'EndExecTime' ) ]

finalStates = ['Done','Completed','Stalled','Failed','Killed']


def __findIndexes( paramNames ):
  indexes = {}
  for k, convList in ( ( 'attrs', attrConv ), ( 'flags', flagConv ), ( 'times', timesConv ) ):
    indexes[ k ] = {}
    for attrPair in convList:
      try:
        iP = paramNames.index( attrPair[1] )
      except ValueError:
        #Not found
        pass
      indexes[ k ][ attrPair[0] ] = iP
  return indexes

def __getJobCounters( selDict ):
  cutDate = selDict.pop('cutDate','')
  result = getRPCClient( "WorkloadManagement/JobMonitoring" ).getCounters( ['Status'], selDict, cutDate)
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  resultDict = {}
  for statusDict, count in result['Value']:
    status = statusDict['Status']
    if status in finalStates:
      resultDict[status] = count
  return resultDict

def __getJobs( selDict, startJob = 0, maxJobs = 500 ):
  result = getRPCClient( "WorkloadManagement/JobMonitoring", group = gOAData.userGroup, userDN = gOAData.userDN ).getJobPageSummaryWeb( selDict,
                                                                                    [( 'JobID', 'DESC' )],
                                                                                    startJob, maxJobs, True )
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  origData = result[ 'Value' ]
  totalRecords = origData[ 'TotalRecords' ]
  retData = { 'entries' : totalRecords, 'jobs' : [] }
  if totalRecords == 0:
    return retData
  indexes = __findIndexes( origData[ 'ParameterNames' ] )
  records = origData[ 'Records' ]
  for record in records:
    job = {}
    for param in indexes[ 'attrs' ]:
      job[ param ] = record[ indexes[ 'attrs' ][ param ] ]
      if param in integerFields:
        job[ param ] = int( float( job[ param ] ) )
    for k in ( 'flags', 'times' ):
      job[ k ] = {}
      for field in indexes[ k ]:
        value = record[ indexes[ k ][ field ] ]
        if value.lower() == "none":
          continue
        if k == 'flags':
          job[ k ][ field ] = value.lower() == 'true'
        else:
          job[ k ][ field ] = value
    retData[ 'jobs' ].append( job )
  return retData


def __getJobDescription( jid ):
  result = getRPCClient( "WorkloadManagement/JobMonitoring" ).getJobJDL( jid )
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
  result = loadJDLAsCFG( result[ 'Value' ] )
  if not result[ 'OK' ]:
    bottle.abort( 500, result[ 'Message' ] )
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
  return jobData


def getWMSClient():
  return WMSClient( getRPCClient( "WorkloadManagement/JobManager" ),
                    getRPCClient( "WorkloadManagement/SandboxStore" ),
                    getTransferClient( "WorkloadManagement/SandboxStore" ) )

@bottle.route( "/jobs", method = 'GET' )
def getJobs():
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  selDict = {}
  startJob = 0
  maxJobs = 100
  for convList in ( attrConv, flagConv ):
    for attrPair in convList:
      jAtt = attrPair[0]
      if jAtt in bottle.request.params:
        selDict[ attrPair[1] ] = List.fromChar( bottle.request[ attr[0] ] )
  if 'allOwners' not in bottle.request.params:
    selDict[ 'Owner' ] = gOAData.userName
  if 'startJob'  in bottle.request:
    try:
      startJob = max( 0, int( bottle.request[ 'startJob' ] ) )
    except:
      bottle.abort( 400, "startJob has to be a positive integer!" )
  if 'maxJobs' in bottle.request:
    try:
      maxJobs = min( 1000, int( bottle.request[ 'maxJobs' ] ) )
    except:
      bottle.abort( 400, "maxJobs has to be a positive integer no greater than 1000!" )

  return __getJobs( selDict, startJob, maxJobs )

@bottle.route( "/jobs/summary" , method = 'GET' )
def getJobsSummary():
  selDict = {}
  # Hard code last day for the time being
  lastUpdate = Time.dateTime() - Time.day
  selDict['cutDate'] = lastUpdate
  return __getJobCounters( selDict )

@bottle.route( "/jobs/:jid", method = 'GET' )
def getJob( jid ):
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  try:
    jid = int( jid )
  except ValueError:
    bottle.abort( 415, "jid has to be an integer! " )
  retDict = __getJobs( { 'JobID' : jid } )
  if retDict[ 'entries' ] == 0:
    bottle.abort( 404, "Unknown jid" )
  return retDict[ 'jobs'][0]

@bottle.route( "/jobs/:jid/description", method = 'GET' )
def getJobDescription( jid ):
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  try:
    jid = int( jid )
  except ValueError:
    bottle.abort( 415, "jid has to be an integer! " )
  return __getJobDescription( jid )


def JSON2JDL( jobData ):
  cfg = CFG.CFG().loadFromDicT( jobData )
  jdl = dumpCFGAsJDL( cfg )
  return jdl



@bottle.route( "/jobs", method = 'POST' )
def postJobs():
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  request = bottle.request
  if len( request.files ):
    result = uploadSandbox( request.files )
    if not result[ 'OK' ]:
      bottle.abort( 500, result[ 'Message' ] )
    isb = result[ 'Value' ]
  else:
    isb = False
  jobs = []
  wms = getWMSClient()
  for k in request.forms:
    origData = bottle.json_lds( request.forms[ k ] )
    jobData = origData
    if isb:
      if 'InputSandbox' not in jobData:
        jobData[ 'InputSandbox' ] = []
      jobData[ 'InputSandbox' ].append( isb )
    cfg = CFG.CFG().loadFromDict( jobData )
    jdl = dumpCFGAsJDL( cfg )
    result = wms.submitJob( jdl )
    if not result[ 'OK' ]:
      bottle.abort( 500, result[ 'Message' ] )
    jobs.append( result[ 'Value' ] )
  return { 'sandbox' : isb, 'jobs' : jobs }


@bottle.route( "/jobs/:jid", method = 'PUT' )
def putJob( jid ):
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  #Modify a job
  pass

@bottle.route( "/jobs/:jid", method = 'DELETE' )
def killJob( jid ):
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  wms = getWMSClient()
  result = wms.killJob( jid )
  if not result[ 'OK' ]:
    if 'NonauthorizedJobIDs' in result:
      bottle.abort( 401, "Not authorized" )
    if 'InvalidJobIDs' in result:
      bottle.abort( 400, "Invalid JID" )
    if 'FailedJobIDs' in result:
      bottle.abort( 500, "Could not delete JID" )

  return { 'jid' : jid }
