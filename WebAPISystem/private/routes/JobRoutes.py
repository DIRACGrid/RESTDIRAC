import bottle
from DIRAC import S_OK, S_ERROR, gLogger

from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData

#GET    SELET
#POST   INSERT
#PUT    UPDATE
#DELETE DELETE

@bottle.route( "/jobs", method = 'GET' )
def getJobs():
  #Get a list of jobs
  pass

@bottle.route( "/jobs", method = 'POST' )
def postJobs():
  #Submit a job
  pass


@bottle.route( "/jobs/:jid", method = 'GET' )
def getJob( jid ):
  #Get a job
  pass

@bottle.route( "/jobs/:jid", method = 'PUT' )
def putJob( jid ):
  #Submit a job
  pass

@bottle.route( "/jobs/:jid", method = 'DELETE' )
def putJob( jid ):
  #Delete a job
  pass
