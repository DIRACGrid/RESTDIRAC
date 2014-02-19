import types
import json
import urllib
import base64
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, RESTHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import List, Time

class BaseFC( RESTHandler ):

  @property
  def rpc( self ):
    return RPCClient( "DataManagement/FileCatalog" )

  def decodeMetadataQuery( self ):
    cond = {}
    for k in self.request.arguments:
      for val in self.request.arguments[ k ]:
        if val.find( "|" ) == -1:
          continue
        val = val.split( "|" )
        op = val[0]
        val = "|".join( val[1:] )
        if 'in' == op:
          val = List.fromChar( val, "," )
        if k not in cond:
          cond[ k ] = {}
        cond[ k ][ op ] = val
    self.log.info( "Metadata condition is %s" % cond )
    return cond

  def intParam( self, key, default = 0 ):
    try:
      return int( self.request.arguments[ key ][-1] )
    except ( KeyError, ValueError ):
      return default

  def decodePath( self, did ):
    try:
      return base64.urlsafe_b64decode( str( did ) ).rstrip( "/" ) or "/"
    except TypeError, e:
      raise WErr( 400, "Cannot decode path" )

  def sanitizeForJSON( self, val ):
    vType = type( val )
    if vType in Time._allTypes:
      return Time.toString( val )
    elif vType == types.DictType:
      for k in val:
        val[ k ] = self.sanitizeForJSON( val[ k ] )
    elif vType == types.ListType:
      for iP in range( len( val ) ):
        val[ iP ] = self.sanitizeForJSON( val[ i ] )
    elif vType == types.TupleType:
      nt = []
      for iP in range( len( val ) ):
        nt[ iP ] = self.sanitizeForJSON( val[ i ] )
      val = tuple( nt )
    return val


