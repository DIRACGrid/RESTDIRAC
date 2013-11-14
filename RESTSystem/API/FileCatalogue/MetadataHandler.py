
import json
import urllib
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK
from RESTDIRAC.RESTSystem.API.FileCatalogue.BaseFC import BaseFC

class MetadataHandler( BaseFC ):

  ROUTE = "/filecatalog/metadata"

  @web.asynchronous
  @gen.engine
  def get( self ):
    cond = self.decodeMetadataQuery()
    result = yield self.threadTask( self.rpc.getMetadataFields )
    if not result[ "OK" ] :
      raise WErr.fromError( result )
    data = result["Value"]
    fields = {}
    for k in data[ 'DirectoryMetaFields' ]:
      fields[ k ] = data[ 'DirectoryMetaFields' ][k].lower()
    result = yield self.threadTask( self.__getRPC().getCompatibleMetadata, cond, "/" )
    if not result[ "OK" ] :
      raise WErr.fromError( result )
    values = result[ 'Value' ]
    data = {}
    for k in fields:
      if k not in values:
        continue
      data[ k ] = { 'type' : fields[k], 'values': values[k] }
    self.finish( data )
