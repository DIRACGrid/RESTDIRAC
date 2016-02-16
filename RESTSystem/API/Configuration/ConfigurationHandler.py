""" Handler to serve the DIRAC configuration data
"""

__RCSID__ = "$Id$"

import json

from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, RESTHandler
from DIRAC import gConfig

class ConfigurationHandler( RESTHandler ):

  ROUTE = "/config"

  @web.asynchronous
  @gen.engine
  def get( self ):

    args = self.request.arguments
    if args.get( 'option' ):
      path = args['option'][0]
      result = yield self.threadTask( gConfig.getOption, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'section' ):
      path = args['section'][0]
      result = yield self.threadTask( gConfig.getOptionsDict, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'options' ):
      path = args['options'][0]
      result = yield self.threadTask( gConfig.getOptions, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'sections' ):
      path = args['sections'][0]
      result = yield self.threadTask( gConfig.getSections, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    else:
      raise WErr( 500, 'Invalid argument' )
