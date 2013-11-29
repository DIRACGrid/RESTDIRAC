
import json
import urllib
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, RESTHandler
from DIRAC.Core.DISET.RPCClient import RPCClient

class FileHandler( RESTHandler ):

  ROUTE = "/filecatalog/file/(id)"

  def __getRPC( self ):
    return RPCClient( "DataManagement/FileCatalog" )

  @web.asynchronous
  @gen.engine
  def get( self, fid ):
    #Get file
    return self.finish()
