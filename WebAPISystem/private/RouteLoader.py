import os
import re
import bottle
import DIRAC

gRoutesRE = re.compile( "(.*Routes)\.py$" )

def loadRoutes():
  routesPath = os.path.join( DIRAC.rootPath, "WebAPIDIRAC", "WebAPISystem", "private", "routes" )
  lastRoutesLoaded = 0
  for fileName in os.listdir( routesPath ):
    match = gRoutesRE.match( fileName )
    if match:
      pythonClass = match.groups()[0]
      objPythonPath = "WebAPIDIRAC.WebAPISystem.private.routes.%s" % pythonClass
      try:
        objModule = __import__( objPythonPath,
                                globals(),
                                locals(), pythonClass )
      except:
        DIRAC.gLogger.exception( "Could not load %s" % fileName )
      routesLoaded = 0
      rules = bottle.app().router.rules
      for rule in rules:
        for method in rules[ rule ]:
          routesLoaded += 1
      routesLoaded -= lastRoutesLoaded
      DIRAC.gLogger.info( "Loaded %s routes for %s" % ( routesLoaded, pythonClass ) )
      lastRoutesLoaded += routesLoaded
      if 'initialize' in dir( objModule ):
        DIRAC.gLogger.notice( "Initializing %s" % pythonClass )
        getattr( objModule, 'initialize' )()
