
import bottle
import json
import pprint
import rauth.service

ListenPort = "2379"
CONSUMER_KEY = "SET HERE YOUR CLIENT/CONSUMER KEY"
#This is the URL of the REST Server
RESTURL = "https://localhost:9910"

oaSrv = rauth.service.OAuth2Service( name = "DIRAC", consumer_key = CONSUMER_KEY, consumer_secret = "",
                                     authorize_url = "%s/oauth2/auth" % RESTURL,
                                     access_token_url = "%s/oauth2/token" % RESTURL )
oaSrv.session.verify = False
oaAccess = False
oaRefresh = False


def checkToken( method ):
  def wrapped( *args, **kwargs ):
    global oaAccess, oaRefresh
    saved = bottle.request.get_cookie( "access_token" )
    if oaAccess:
      if not saved:
        bottle.response.set_cookie( 'access_token', oaAccess )
        bottle.response.set_cookie( 'refresh_token', oaRefresh )
    else:
      if not saved:
        bottle.redirect( "/oa/request" )
      oaAccess = saved
      oaRefresh = bottle.request.get_cookie( "refresh_token" )
    return method( *args, **kwargs )
  return wrapped

def RESTquest( method, url, params = None, data = None, files = None ):
  headers = None
  url = url.lstrip( "/" )
  if method == 'GET' and params:
    params = dict( ( k, params[k] ) for k in params )
  if method not in ( 'POST', 'PUT' ) and params:
    params[ 'access_token' ] = oaAccess
  else:
    headers = { 'Authorization': "bearer %s" % oaAccess }
  if not params:
    params = {}
  if not data:
    data = {}
  if not files:
    files = {}
  #resp = oaSrv.request( method, "%s/%s" % ( RESTURL, url ), params = params, headers = headers, data = data, files = files )
  resp = oaSrv.request( method, "%s/%s" % ( RESTURL, url ), params = params, headers = headers, data = data, files = files,
                        cert = ( "/Users/adria/.globus/usercert.pem", "/Users/adria/.globus/userkey.pem" ) )
  if resp.response.status_code != 200:
    return showError( resp.response )
  data = resp.content
  return "<html><body><a href='/'>Go home</a><hr/><pre style='border:1px black;'>%s</pre></body></html>" % pprint.pformat( data )


@bottle.route( "/oa/request" )
def oaRequest():
  print "REQUESTING TOKEN..."
  bottle.redirect( oaSrv.get_authorize_url( redirect_uri = "http://localhost:%s/oa/grant" % ListenPort, response_type = 'code' ) )

@bottle.route( "/oa/grant" )
def oaGrant():
  print "RECEIVED GRANT!!"
  global oaAccess, oaRefresh
  if 'code' not in bottle.request.params:
    bottle.abort( 400, "Missing code" )

  data = dict( code = bottle.request.params[ 'code' ], grant_type = 'authorization_code', redirect_uri = 'http://localhost:%s/oa/grant' % ListenPort )
  print "ABOUT TO GET ACCESS TOKEN WITH", data
  resp = oaSrv.get_access_token( 'POST', data = data )
  print "DONE", resp
  if resp.response.status_code != 200:
    bottle.redirect( "/" )
  res = resp.content
  if 'error' in res:
    return "ERROR GETTING TOKEN: %s<br/><a href='/'>Go Home</a>" % res[ 'error' ]
  oaAccess = res[ 'access_token' ]
  oaRefresh = res[ 'refresh_token' ]
  oaSrv.access_token = oaAccess
  bottle.redirect( "/" )


def showError( response ):
  return "ERROR %s: %s" % ( response.status_code, response.text )

@bottle.route( "/reset" )
@checkToken
def reset():
  global oaAccess
  oaAccess = False
  bottle.response.set_cookie( 'access_token', "" )
  bottle.response.set_cookie( 'refresh_token', "" )
  bottle.redirect( "/" )




@bottle.route( "/jobs" )
@checkToken
def jobs():
  return RESTquest( "GET", "/jobs", bottle.request.params )

@bottle.route( "/jobs/:jid" )
@checkToken
def jobsjid( jid ):
  return RESTquest( "GET", "/jobs/%s" % jid, bottle.request.params )

@bottle.route( "/jobs/:jid/:obj" )
@checkToken
def jobsjid( jid, obj ):
  return RESTquest( "GET", "/jobs/%s/%s" % ( jid, obj ), bottle.request.params )

@bottle.route( "/submitjobform" )
@checkToken
def submitJobForm():
  return """
<html>
 <body>
  <form method='post' action='dosubmitjob' enctype='multipart/form-data'>
   Job name: <input type='text' name='name' value='Job name'/><br/>
   Executable: <input type='text' name='exec' value='/bin/echo'/><br/>
   Arguments: <input type='text' name='args' value='Hello World'/><br/>
   File 1: <input type='file' name='f1'/><br/>
   File 2:<input type='file' name='f2'/><br/>
   <input type='submit' />
  </form>
 </body>
</html>
"""

@bottle.route( "/dosubmitjob", method = 'POST')
@checkToken
def doSubmitJob():
  params = bottle.request.params
  manifest = { 'name' : params[ 'name' ],
               'executable' : params[ 'exec' ],
               'arguments' : params[ 'args' ]
             }
  data = { 'manifest' : json.dumps( manifest ) }
  return RESTquest( 'POST', "/jobs", data = data, files = bottle.request.files )

@bottle.route( "/" )
@checkToken
def index():
  return """
<html>
 <head><title>Example DIRAC REST API</title></head>
 <body>
  <h4>Your token is %s</h4>
  <br/>
  <a href='/jobs'>List my jobs</a><br/>
  <a href='/jobs?allOwners=true'>List all jobs</a><br/>
  <a href='/submitjobform'>Submit a job</a><br/>
  <a href='/reset'>Reset</a><br/>
 </body>
</html>
""" % oaAccess


bottle.debug( True )
bottle.run( host = 'localhost', port = ListenPort, reloader = True )
