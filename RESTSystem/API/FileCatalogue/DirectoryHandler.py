
import json
import urllib
import base64
from tornado import web, gen
from DIRAC.Core.Utilities import List
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, RESTHandler
from RESTDIRAC.RESTSystem.API.FileCatalogue.BaseFC import BaseFC


class DirectoryHandler( BaseFC ):

  ROUTE = "/filecatalogue/directory(?:/([a-zA-Z0-9=-_]+)(?:/([a-z]+))?)?"

  @web.asynchronous
  def get( self, did, obj ):
    if not obj:
      self.__listDir( did )
    elif 'metadata' == obj:
      #Search compatible metadata for this directory
      self.__getCompatibleMetadata( did )
    elif 'search' == obj:
      #Search directories with metadata restrictions
      self.__search( did )
    else:
      raise WErr( 404, "WTF?" )

  @gen.engine
  def __getCompatibleMetadata( self, did ):
    path = self.decodePath( did )
    cond = self.decodeMetadataQuery()
    result = yield self.threadTask( self.rpc.getCompatibleMetadata, cond, path )
    if not result[ "OK" ] :
      raise WErr.fromError( result )
    self.finish( self.sanitizeForJSON( result[ 'Value' ] ) )


  def __filterChildrenOf( self, root, dirDict ):
    filtered = []
    for did in list( dirDict ):
      path = dirDict[ did ]
      if len( path ) > len( root ) or path == root:
        filtered.append( path )
    return filtered

  def __buildDirTree( self, root, dirData ):
    rootD = { 'f': 0, 's' : 0 }
    for dpath in dirData:
      ddata = dirData[ dpath ]
      cpath = dpath[ len( root ): ]
      if not cpath and dpath != root:
        continue
      dT = rootD
      for level in List.fromChar( cpath, "/" ):
        if 'd' not in dT:
          dT[ 'd' ] = {}
        dT = dT[ 'd' ]
        if level not in dT:
          dT[ level ] = {}
        dT = dT[ level ]
      dT[ 'f' ] = ddata[ 'LogicalFiles' ]
      dT[ 's' ] = ddata[ 'LogicalSize' ]
    return rootD

  @gen.engine
  def __search( self, did ):
    path = self.decodePath( did )
    cond = self.decodeMetadataQuery()
    result = yield self.threadTask( self.rpc.findDirectoriesByMetadata, cond, path )
    if not result[ 'OK' ]:
      raise WErr.fromError( result )
    data = self.__filterChildrenOf( path, result[ 'Value' ] )
    result = yield self.threadTask( self.rpc.getDirectorySize, data, False, False )
    if not result[ 'OK' ]:
      raise WErr.fromError( result )
    tree = self.__buildDirTree( path, result[ 'Value' ][ 'Successful' ] )
    self.finish( self.sanitizeForJSON( tree ) )

  @gen.engine
  def __listDir( self, did ):
    path = self.decodePath( did )
    try:
      pageSize = max( 0, int( self.request.arguments[ 'page_size' ][-1] ) )
    except ( ValueError, KeyError ):
      pageSize = 0
    try:
      verbose = bool( self.request.arguments[ 'extra' ][-1] )
    except KeyError:
      verbose = False
    result = yield self.threadTask( self.rpc.listDirectory, path, verbose )
    if not result[ 'OK' ]:
      self.log.error( "Cannot list directory for %s:%s" % ( path, result[ 'Message' ] ) )
      raise WErr.fromError( result )
    data = result[ 'Value' ]
    if not path in data[ 'Successful' ]:
      raise WErr( 404, data[ 'Failed' ][ path ] )
    contents = data[ 'Successful' ][ path ]
    ch = {}
    for kind in contents:
      ch[ kind ] = {}
      for sp in contents[ kind ]:
        ch[ kind ][ sp[ len( path ) + 1: ] ] = contents[ kind ][ sp ]
    self.finish( self.sanitizeForJSON( ch ) )

  @web.asynchronous
  def post( self, did, obj ):
    if not obj:
      self.__createDir( did )
    else:
      raise WErr( 404, "WTF?" )

  @gen.engine
  def __createDir( self, did ):
    path = self.decodePath( did )
    result = yield self.threadTask( self.rpc.createDirectory, path )
    if not result[ 'OK' ]:
      raise WErr.fromError( result )
    data = result[ 'Value' ]
    if path in data[ 'Successful' ]:
      self.finish( data[ 'Successful' ][ path ] )
    else:
      raise WErr( 404, data[ 'Failed' ][ path ] )

  @web.asynchronous
  def delete( self, did, obj ):
    if not obj:
      self.__deleteDir( did )
    else:
      raise WErr( 404, "WTF?" )

  @gen.engine
  def __deleteDir( self, did ):
    path = self.decodePath( did )
    result = yield self.threadTask( self.rpc.removeDirectory, path )
    if not result[ 'OK' ]:
      if result[ 'Message' ].lower().find( "not exist" ) > -1:
        raise WErr( 404, result[ 'Message' ] )
      else:
        raise WErr.fromError( result )
    self.finish( { path: True } )


