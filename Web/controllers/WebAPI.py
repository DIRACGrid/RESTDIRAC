import logging
import time
import random
import oauth2
import urllib

try:
  from hashlib import md5
except:
  from md5 import md5

from dirac.lib.base import *
from dirac.lib.credentials import getUserDN, getSelectedGroup, getSelectedSetup
from dirac.lib.webBase import defaultRedirect

from WebAPIDIRAC.WebAPISystem.private.OAManager import gOAManager, gOAData

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
log = logging.getLogger( __name__ )

random.seed()

class WebapiController( BaseController ):


  def authorizeRequest( self ):
    params = {}
    for key in request.params:
      params[ key ] = request.params[ key ]
    gOAManager.parse( method = request.method,
                      url = request.url,
                      headers = request.headers,
                      parameters = params,
                      query_string = request.query_string )
    if not gOAData.token:
      c.error = "Dude! You shouldn't be here without a request token :P"
      return render( "/error.mako" )
    if not getUserDN() or getSelectedGroup() == "visitor":
      c.error = "Please log in with your certificate"
      return render( "/error.mako" )
    result = gOAManager.credentials.getRequestData( gOAData.token )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    reqData = result[ 'Value' ]
    consData = reqData[ 'consumer' ]
    result = gOAManager.credentials.generateVerifier( consData[ 'key' ],
                                                         reqData[ 'request' ],
                                                         c.userDN, c.userGroup )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    verifier = result[ 'Value' ]

    c.consName = consData[ 'name' ]
    c.consImg = consData[ 'icon' ]
    c.userDN = getUserDN()
    c.userGroup = getSelectedGroup()
    c.request = gOAData.token
    c.verifier = verifier
    c.consumerKey = consData[ 'key' ]

    c.callback = gOAData.callback or reqData[ 'callback' ]
    c.callback = urllib.quote_plus( c.callback )

    return render( "/WebAPI/authorizeRequest.mako" )

  def __denyAccess( self ):
    gOAManager.credentials.expireVerifier( gO )

  def grantAccess( self ):
    try:
      verifier = str( request.params[ 'verifier' ] )
    except ValueError:
      c.error = "Missing verifier in query!"
      return render( "/error.mako" )
    try:
      requestToken = str( request.params[ 'request' ] )
      consumerKey = str( request.params[ 'consumerKey' ] )
      lifeTime = int( request.params[ 'accessTime' ] ) * 3600
    except Exception, excp:
      gOAManager.credentials.deleteVerifier( verifier )
      c.error = "Missing or invalid in query!<br/>%s" % str( excp )
      return render( "/error.mako" )

    if 'grant' not in request.params or str( request.params[ 'grant' ] ) != "Grant":
      log.info( "Access denied for token" )
      gOAManager.credentials.deleteVerifier( verifier )
      return defaultRedirect()


    result = gOAManager.credentials.setVerifierProperties( consumerKey, requestToken, verifier,
                                                  getUserDN(), getSelectedGroup(), lifeTime )
    if not result['OK']:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )

    result = gOAManager.credentials.getRequestData( requestToken )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    reqData = result[ 'Value' ]
    consData = reqData[ 'consumer' ]

    oaConsumer = oauth2.Consumer( consData[ 'key' ], consData[ 'secret' ] )
    oaToken = oauth2.Token( reqData[ 'request' ], reqData[ 'secret' ] )
    oaToken.set_verifier( verifier )

    oaRequest = oauth2.Request.from_consumer_and_token( oaConsumer,
                                                        oaToken,
                                                        http_method = 'GET',
                                                        http_url = reqData[ 'callback' ] )
    oaRequest.sign_request( oauth2.SignatureMethod_HMAC_SHA1(), oaConsumer, oaToken )

    return redirect_to( str( oaRequest.to_url() ) )


