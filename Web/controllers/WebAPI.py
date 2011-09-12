import logging
import time
import random
import oauth2

try:
  from hashlib import md5
except:
  from md5 import md5

from dirac.lib.base import *
from dirac.lib.credentials import getUserDN, getSelectedGroup, getSelectedSetup
from dirac.lib.webBase import defaultRedirect

from WebAPIDIRAC.WebAPISystem.private.OAuthHelper import OAuthHelper

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
log = logging.getLogger( __name__ )

random.seed()

class WebapiController( BaseController ):

  oaHelper = OAuthHelper()

  def authorizeRequest( self ):
    params = {}
    for key in request.params:
      params[ key ] = request.params[ key ]
    oaRequest = oauth2.Request.from_request( request.method,
                                             request.url,
                                             headers = request.headers,
                                             parameters = params,
                                             query_string = request.query_string )
    if not oaRequest or 'oauth_token' not in oaRequest:
      c.error = "Dude! You shouldn't be here without a request token :P"
      return render( "/error.mako" )
    requestToken = oaRequest[ 'oauth_token' ]
    if not getUserDN() or getSelectedGroup() == "visitor":
      c.error = "Please log in with your certificate"
      return render( "/error.mako" )
    result = self.oaHelper.getRequestData( requestToken )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    reqData = result[ 'Value' ]
    print reqData
    result = self.oaHelper.getConsumerData( reqData[ 'consumerKey' ] )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    consData = result[ 'Value' ]
    c.consName = consData[ 'name' ]
    c.consImg = consData[ 'icon' ]
    c.userDN = getUserDN()
    c.userGroup = getSelectedGroup()
    c.request = requestToken
    c.ticket = md5( "%s%s%s%s" % ( str( consData ), requestToken, time.time(), random.random() ) ).hexdigest()
    session[ 'oaAuthTicket' ] = c.ticket
    return render( "/WebAPI/authorizeRequest.mako" )

  def __denyAccess( self ):
    pass

  def grantAccess( self ):
    if 'grant' not in request.params or str( request.params[ 'grant' ] ) != "true":
      self.__denyAccess()
      return defaultRedirect()
    try:
      requestToken = str( request.params[ 'request' ] )
    except KeyError:
      self.__denyAccess()
      return defaultRedirect()
    try:
      lifeTime = int( request.params[ 'accessTime' ] ) * 3600
    except:
      self.__denyAccess()
      return defaultRedirect()
    result = self.oaHelper.getRequestData( requestToken )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    reqData = result[ 'Value' ]
    print reqData
    result = self.oaHelper.getConsumerData( reqData[ 'consumerKey' ] )
    if not result[ 'OK' ]:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    consData = result[ 'Value' ]
    result = oaHelper.generateRequestVerifier( reqData[ 'consumerKey' ], requestToken )
    if not result['OK']:
      c.error = result[ 'Message' ]
      return render( "/error.mako" )
    verifier = result[ 'Value' ]
    oaConsumer = oauth2.Consumer( consData[ 'key' ], consData[ 'secret' ] )
    oaToken = oauth2.Token( requestToken, reqData[ 'secret' ] )
    oaToken.set_verifier( verifier )
    oaRequest = oauth2.Request.from_consumer_and_token( oaConsumer, oaToken, http_method = 'GET', http_url = consData[ 'callback' ] )
    oaRequest.sign_request( oauth2.SignatureMethod_HMAC_SHA1(), oaConsumer, oaToken )
    return redirect( oaRequest.to_url() )


