import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security import Properties
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from RESTDIRAC.RESTSystem.DB.OATokenDB import OATokenDB
from RESTDIRAC.RESTSystem.Client import OAToken

class OATokenStoreHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    try:
      cls.tokenDB = OATokenDB()
    except RuntimeError, excp:
      return S_ERROR( "Could not connect to DB: %s" % excp )
    result = cls.tokenDB._getConnection()
    if not result[ 'OK' ]:
      cls.log.warn( "Could not connect to OAtokenDB (%s). Resorting to RPC" % result[ 'Message' ] )
    result[ 'Value' ].close()
    #Try to do magic
    myStuff = dir( cls )
    for method in OAToken.__remoteMethods__:
      if method.find( "get" ) != 0 and method.find( "generate" ) != 0:
        continue
      if "export_%s" % method in myStuff:
        cls.log.info( "Wrapping method %s. It's already defined in the Handler" % method )
      else:
        cls.log.info( "Mimicking method %s" % method )
        setattr( cls, "auth_%s" % method, [ 'authenticated' ] )
        setattr( cls, "types_%s" % method, [ types.TupleType ] )
        setattr( cls, "export_%s" % method, cls.__mimeticFunction )
    return S_OK()

  def __unwrapArgs( self, margs ):
    if len( margs ) < 1 or type( margs[0] ) != types.TupleType or ( len( margs ) > 1 and type( margs[1] ) != types.DictType ):
      return S_ERROR( "Invalid arg stub. Expected tuple( args, kwargs? ), received %s" % str( margs ) )
    if len( margs ) == 1:
      return S_OK( ( margs[0], {} ) )
    else:
      return S_OK( ( margs[0], margs[1] ) )

  def __mimeticFunction( self, margs ):
    method = self.srv_getActionTuple()[1]
    result = self.__unwrapArgs( margs )
    if not result[ 'OK' ]:
      return result
    args, kwargs = result[ 'Value' ]
    #DO PROPER AUTHENTICATION
    if not self.__clientHasAccess():
      return S_ERROR( "You're not authorized to access tokens" )
    return getattr( self.__getOAToken(), method )( *args, **kwargs )

  def __unwrapAndCall( self, margs ):
    method = self.srv_getActionTuple()[1]
    result = self.__unwrapArgs( margs )
    if not result[ 'OK' ]:
      return result
    args, kwargs = result[ 'Value' ]
    if not self.__clientHasAccess():
      return S_ERROR( "You're not authorized to access tokens" )
    return getattr( self, "_usr_def_%s" % method )( *args, **kwargs )

  def __getOAToken( self ):
    return OAToken.OAToken( forceLocal = True )

  def __clientHasAccess( self ):
    return True
