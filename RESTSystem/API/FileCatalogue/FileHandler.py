
import json
import urllib
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK
from RESTDIRAC.RESTSystem.API.FileCatalogue.BaseFC import BaseFC

class FileHandler( BaseFC ):

  ROUTE = "/filecatalogue/file/([a-zA-Z0-9=-_]+)(?:/([a-z]+))?"

  def __getRPC( self ):
    return RPCClient( "DataManagement/FileCatalog" )

  @web.asynchronous
  def get( self, fid, obj ):
    if obj == "attributes":
      self.__getAttributes( fid )
    elif obj == "metadata":
      self.__getMetadata( fid )
    else:
      raise WErr( 404, "WTF?" )

  @gen.engine
  def __getAttributes( self, fid ):
    path = self.decodePath( fid )
    result = yield self.threadTask( self.rpc.getFileMetadata, path )
    if not result[ "OK" ] or path not in result[ 'Value' ][ 'Successful' ]:
      raise WErr.fromError( result )
    self.finish( self.sanitizeForJSON( result[ 'Value' ][ 'Successful' ][ path ] ) )

  @gen.engine
  def __getMetadata( self, fid ):
    path = self.decodePath( fid )
    result = yield self.threadTask( self.rpc.getFileUserMetadata, path )
    if not result[ "OK" ] :
      raise WErr.fromError( result )
    self.finish( self.sanitizeForJSON( result[ 'Value' ] ) )
