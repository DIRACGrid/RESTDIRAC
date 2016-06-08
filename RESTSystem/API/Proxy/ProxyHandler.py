""" Handler to manage proxies in the ProxyManager service
"""

__RCSID__ = "$Id$"

import json
import tempfile
import os

from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, RESTHandler
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities import Time
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername, getGroupsForDN
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.Subprocess import shellCall

class ProxyHandler( RESTHandler ):

  ROUTE = "/proxy/([a-z]+)/([a-z,_]+)"

  def _getProxies( self, user, group ):

    selDict = {}
    if user.lower() != 'all':
      selDict = { "UserName": user }

    result = gProxyManager.getDBContents( selDict )
    if not result['OK']:
      return WErr( 500, result['Message'] )

    records = result['Value']['Records']

    resultList = []

    for record in records:
      if ( record[0] == user or user.lower() == 'all' ) and \
         ( record[2] == group or group.lower() == 'all' ):
        validity = Time.toString( record[3] )
        resultList.append( { "User": record[0],
                             "UserDN": record[1],
                             "Group": record[2],
                             "Validity": validity } )

    return WOK( resultList )

  @web.asynchronous
  @gen.engine
  def get( self, user, group ):

    result = yield self.threadTask( self._getProxies, user, group )
    if not result.ok:
      raise result
    data = result.data

    self.finish( json.dumps( data ) )

  def _uploadProxy( self, user, group, password, files ):

    proxyChain = X509Chain()
    if 'cert' in files:
      proxyChain.loadChainFromString( files['cert'][0]['body'] )
      proxyChain.loadKeyFromString( files['key'][0]['body'], password )
    elif 'p12' in files:

      filep12 = tempfile.mktemp( suffix = '.p12' )
      with open( filep12, 'w' ) as outp12:
        outp12.write( files['p12'][0]['body'] )
      cmd = "openssl pkcs12 -in %s -clcerts -password pass:'%s' -passout pass:'%s'" % ( filep12, password, password )
      try:
        result = shellCall( 0, [ cmd ] )
      except Exception as exc:
        return WErr( 500, "Exception while p12 coversion" )
      finally:
        os.unlink( filep12 )
      if not result['OK']:
        return WErr( 500, "Failed to convert p12 certificate" )
      status, output, error = result['Value']
      if status:
        return WErr( 500, "ERROR: %s" % error )
      proxyChain.loadChainFromString( output )
      proxyChain.loadKeyFromString( output, password )
    else:
      return WErr( 500, "No certificate files provided" )

    result = proxyChain.getCredentials()
    if not result['OK']:
      return WErr( 500, "Invalid certificate" )
    p12UserName = result['Value']['username']
    p12UserDN = result['Value']['subject']
    if user.lower() != 'unknown':
      if user != p12UserName:
        return WErr( 500, "Requested user name does not match the p12 certificate" )
    if group.lower() == "all":
      result = proxyChain.getCredentials()
      if not result['OK']:
        return WErr( 500, result['Message'] )
      userDN = result['Value']['subject']
      result = getGroupsForDN( userDN )
      if not result['OK']:
        return WErr( 500, result['Message'] )
      groups = result['Value']
    else:
      groups = [group]

    for group in groups:
      result = gProxyManager.uploadProxy( proxyChain, group  )
      if not result['OK']:
        return WErr( 500, result['Message'] )

    resultDict = {}
    resultDict['UserDN'] = p12UserDN
    resultDict['UserName'] = p12UserName
    resultDict['Groups'] = result['Value'][p12UserDN]

    return WOK( resultDict )

  def post( self, user, group ):

    files = self.request.files
    password = self.request.arguments['Password'][0]

    result = self._uploadProxy( user, group, password, files )
    if not result.ok:
      raise result
    data = result.data
    self.finish( { 'UserName': data['UserName'],
                   'UserDN': data['UserDN'],
                   'UploadedProxies': data['Groups'].keys() } )
