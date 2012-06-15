import bottle
import datetime
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData, gOAManager
from WebAPIDIRAC.WebAPISystem.private.Clients import getRPCClient, getTransferClient
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.Utilities import List, CFG
import DIRAC.Core.Utilities.Time as Time

#GET    SELET
#POST   INSERT
#PUT    UPDATE
#DELETE DELETE


@bottle.route( "/jobs/history", method = 'GET' )
def getJobsHistory():
  result = gOAManager.authorize()
  if not result[ 'OK' ]:
    bottle.abort( 401, result[ 'Message' ] )
  condDict = {}
  if 'onlyme' in bottle.request.params:
    condDict[ 'User' ] = gOAData.userName
  timeSpan = 86400
  if 'timeSpan' in bottle.request.params:
    try:
      timeSpan = max( 86400, int( bottle.request.params[ 'timeSpan' ] ) )
    except ValueError:
      bottle.abort( 400, "timeSpan has to be an integer!" )
  rpg = ReportsClient( rpcClient = getRPCClient("Accounting/ReportGenerator"), transferClient = getTransferClient("Accounting/ReportGenerator") )
  end = datetime.datetime.utcnow()
  start = end - datetime.timedelta( seconds = timeSpan )
  result = rpg.getReport( "WMSHistory", "NumberOfJobs", start, end, condDict, 'Status' )
  if not result[ 'OK' ]:
    bottle.abort( 500, "Server Error: %s" % result[ 'Message' ] )
  return result[ 'Value' ]  
  

