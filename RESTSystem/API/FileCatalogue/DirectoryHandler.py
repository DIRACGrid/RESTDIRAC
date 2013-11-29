
import json
import urllib
import base64
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, RESTHandler
from RESTDIRAC.RESTSystem.API.FileCatalogue.BaseFC import BaseFC


class DirectoryHandler( BaseFC ):

  ROUTE = "/filecatalogue/directory(?:/([a-zA-Z0-9=-_]+)(?:/([a-z]+))?)?"

  @web.asynchronous
  def get( self, did, obj ):
    if not obj:
      self.__searchDirs( did )
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
    self.finish( result[ 'Value' ] )

  @gen.engine
  def __search( self, did ):
    path = self.decodePath( did )
    cond = self.decodeMetadataQuery()
    start = self.intParam( 'start', 0 )
    limit = self.intParam( 'limit', 1000 )
    result = yield self.threadTask( self.rpc.findDirectoriesByMetadata, cond, path )
    if not result[ 'OK' ]:
      raise WErr.fromError( result )
    data = result[ 'Value' ]
    self.finish( { 'Directories': [ data[k] for k in data ] } )


  @gen.engine
  def __searchDirs( self, did ):
    if not did:
      path = "/"
    else:
      path = self.decodePath( did )
    result = yield self.threadTask( self.rpc.listDirectory, path, False )
    if not result[ 'OK' ]:
      self.log.error( "Cannot list directory for %s:%s" % ( path, result[ 'Message' ] ) )
      raise WErr.fromError( result )
    data = result[ 'Value' ]
    if path in data[ 'Successful' ]:
      self.finish( self.sanitizeForJSON( data ) )
    else:
      raise WErr( 404, data[ 'Failed' ][ path ] )
