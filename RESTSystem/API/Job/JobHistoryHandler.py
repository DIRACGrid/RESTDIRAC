""" Job history handler
"""

import datetime
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, RESTHandler
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient

__RCSID__ = "$Id$"

class JobHistoryHandler( RESTHandler ):

  ROUTE = "/jobs/history"

  @web.asynchronous
  @gen.engine
  def get( self ):
    condDict = {}
    if 'allOwners' not in self.request.arguments:
      condDict[ 'Owner' ] = self.getUserName()
    timespan = 86400
    if 'timeSpan' in self.request.arguments:
      try:
        timespan = int( self.request.arguments[ 'timeSpan' ][-1] )
      except ValueError:
        raise WErr( 400, reason = "timeSpan has to be an integer!" )
    rpc = ReportsClient()
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta( seconds = timespan )
    result = yield self.threadTask( rpc.getReport, "WMSHistory", "NumberOfJobs", start, end, condDict, "Status" )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      raise WErr( 500 )
    data = result[ 'Value' ]
    self.finish( data )
