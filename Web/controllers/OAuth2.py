import logging
import time
import random
import urllib
import base64
import cgi

try:
  from hashlib import md5
except:
  from md5 import md5

from dirac.lib.base import *
from dirac.lib.credentials import getUserDN, getSelectedGroup, getSelectedSetup
from dirac.lib.webBase import defaultRedirect

from RESTDIRAC.RESTSystem.Client.OAToken import OAToken

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
log = logging.getLogger( __name__ )

random.seed()

class Oauth2Controller( BaseController ):

  __oaToken = OAToken()

  def __getMap( self ):
    url = request.url
    url = url[ url.find( "/", 8 ) : ]
    scriptName = request.environ[ 'SCRIPT_NAME' ]
    if scriptName:
      if scriptName[0] != "/":
        scriptName = "/%s" % scriptName
      if url.find( scriptName ) == 0:
        url = url[ len( scriptName ): ]
    pI = url.find( '?' );
    if pI > -1:
      params = url[ pI + 1: ]
      url = url[ :pI ]
    else:
      params = ""
    pDict = dict( cgi.parse_qsl( params ) )
    if not url:
      url = "/"
    return ( config[ 'routes.map' ].match( url ), pDict )

  def __changeSetupAndRedirect( self, requestedValue ):
    redDict = False
    refDict, paramsDict = self.__getMap()
    if refDict:
      redDict = paramsDict
      for key in ( 'controller', 'action' ):
        if key in refDict:
          redDict[ key ] = refDict[ key ]
      if 'id' in refDict:
        redDict[ 'id' ] = refDict[ 'id' ]
      else:
        redDict[ 'id' ] = None
      if 'controller' in redDict and 'action' in redDict and \
         redDict[ 'controller' ] == 'template' and \
         redDict[ 'action' ] == 'view':
        redDict = False
    request.environ[ 'pylons.routes_dict' ][ 'dsetup' ] = requestedValue
    if redDict:
      return redirect_to( **redDict )
    return defaultRedirect()

  def index( self ):
    return redirect_to( action = "authorizeCode" )

  def authorizeCode( self ):
    #Check the setup is allright
    if 'setup' in request.params:
      requestedSetup = request.params[ 'setup' ]
      result = gConfig.getSections( "/DIRAC/Setups" )
      if not result[ 'OK' ]:
        c.error( "Oops: %s" % result[ 'Value' ] )
        return render( "/error.mako" )
      knownSetups = result[ 'Value' ]
      if requestedSetup not in knownSetups:
        c.error = "Unknown %s setup" % requestedSetup
        return render( "/error.mako" )
      if getSelectedSetup() != requestedSetup:
        return self.__changeSetupAndRedirect( requestedSetup )

    try:
      cid = str( request.params[ 'client_id' ] )
    except KeyError:
      c.error = "Dude! You shouldn't be here without a client_id :P"
      return render( "/error.mako" )
    if not getUserDN() or getSelectedGroup() == "visitor":
      c.error = "Please log in with your certificate"
      return render( "/error.mako" )
    result = self.__oaToken.getClientDataByID( cid )
    if not result[ 'OK' ]:
      c.error = "Unknown originating client"
      return render( "/error.mako" )
    cData = result[ 'Value' ]

    try:
      redirect = str( request.params[ 'redirect_uri' ] )
    except KeyError:
      redirect = ""

    c.cName = cData[ 'Name' ]
    c.cImg = cData[ 'Icon' ]
    c.cID = cid
    if c.redirect:
      c.redirect = base64.urlsafe_b64encode( redirect )
    c.userDN = getUserDN()
    c.userGroup = getSelectedGroup()

    for k in ( 'scope', 'state' ):
      if k in request.params:
        setattr( c, k, str( request.params[k] ) )
      else:
        setattr( c, k, False )

    return render( "/OAuth2/authorizeCode.mako" )

  def grantAccess( self ):
    uDN = getUserDN()
    uGroup = getSelectedGroup()
    if not uDN or uGroup == "visitor":
      c.error = "Please log in with your certificate"
      return render( "/error.mako" )
    try:
      cid = str( request.params[ 'cid' ] )
      lifeTime = int( request.params[ 'accessTime' ] ) * 3600
      qRedirect = str( request.params[ 'redirect' ] )
      if qRedirect:
        qRedirect = base64.urlsafe_b64decode( qRedirect )
    except Exception, excp:
      c.error = "Missing or invalid in query!"
      return render( "/error.mako" )

    result = self.__oaToken.getClientDataByID( cid )
    if not result[ 'OK' ]:
      c.error = "OOps... Seems the client id didn't make it :P"
      return render( "/error.mako" )
    cData = result[ 'Value' ]

    if qRedirect:
      redirect = qRedirect
    else:
      redirect = cData[ 'Redirect' ]

    if 'grant' not in request.params or str( request.params[ 'grant' ] ) != "Grant":
      return redirect_to( redirect, error = "access_denied" )

    kw = { 'cid' : cid, 'userDN' : uDN, 'userGroup' : uGroup, 'lifeTime' : lifeTime }
    if 'scope' in request.params:
      kw[ 'scope' ] = str( request.params[ 'scope' ] )
    if qRedirect:
      kw[ 'redirect' ] = qRedirect

    result = self.__oaToken.generateCode( **kw )
    if not result[ 'OK' ]:
      c.error = "Could not generate token: %s" % result[ 'Message' ]
      return render( "/error.mako" )

    res = { 'code' : result[ 'Value' ] }
    if 'state' in request.params:
      res[ 'state' ] = str( request.params[ 'state' ] )
    redirect_to( redirect, **res )

