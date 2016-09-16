import types
import os
import shutil
import json
from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, WOK, TmpDir, RESTHandler
from RESTDIRAC.ConfigurationSystem.Client.Helpers import RESTConf

class CSHandler( RESTHandler ):

  ROUTE = "/config/(Sections|Options|Value)"

  @web.asynchronous
  def get( self, reqType ):
    if reqType == "Sections":
      return self.SectionsAction()
    elif reqType == "Options":
      return self.OptionsAction()
    elif reqType == "Value":
      return self.ValueAction()


  @gen.engine
  def ValueAction( self ):
    args = self.request.arguments
    try:
      path = args[ 'ValuePath' ][0]
    except KeyError:
      self.send_error( 400 )
      return
    condDict = {}
    if 'allOwners' not in self.request.arguments:
      condDict[ 'Owner' ] = self.getUserName()
    result = RESTConf.getValue( path )
    self.finish( result )