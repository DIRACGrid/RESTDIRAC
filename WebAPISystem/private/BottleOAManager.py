

import bottle
import urlparse

from WebAPIDIRAC.WebAPISystem.private.OAManager import OAManager, gOAData
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from WebAPIDIRAC.WebAPISystem.Client.CredentialsWrapper import getCredentialsClient

class BottleOAManager( OAManager ):

  def __init__( self ):
    super( BottleOAManager, self ).__init__()

  def parse( self ):
    request = bottle.request

    urlparts = request.urlparts
    url = urlparse.urlunsplit( ( urlparts[0], urlparts[1], urlparts[2], "", "" ) )

    return super( BottleOAManager, self ).parse( request.method,
                                                    url,
                                                    request.headers,
                                                    request.params,
                                                    request.query_string )

  def notAuthorized( self ):
    bottle.abort( 500 )



gOAManager = BottleOAManager()
