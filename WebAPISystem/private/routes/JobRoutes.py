import bottle, simplejson
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData
from WebAPIDIRAC.WebAPISystem.private.Clients import getRPCClient
from DIRAC.Core.Utilities import List

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


def __getJobs( selDict, startJob = 0, maxJobs = 500 ):
  result = getRPCClient( "WorkloadManagement/JobMonitoring" ).getJobPageSummaryWeb( selDict,
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

@bottle.route( "/jobs", method = 'GET' )
def getJobs():
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
      maxJobs = min( 500, int( bottle.request[ 'maxJobs' ] ) )
    except:
      bottle.abort( 400, "maxJobs has to be a positive integer no greater than 500!" )

  return __getJobs( selDict, startJob, maxJobs )

@bottle.route( "/jobs", method = 'POST' )
def postJobs():
  #Submit a job
  pass


@bottle.route( "/jobs/:jid", method = 'GET' )
def getJob( jid ):
  retDict = __getJobs( { 'JobID' : jid } )
  print retDict
  if retDict[ 'entries' ] == 0:
    bottle.abort( 404, "Unknown jid" )
  return retDict[ 'jobs'][0]


@bottle.route( "/jobs/:jid", method = 'PUT' )
def putJob( jid ):
  #Modify a job
  pass

@bottle.route( "/jobs/:jid", method = 'DELETE' )
def putJob( jid ):
  #Delete a job
  pass
