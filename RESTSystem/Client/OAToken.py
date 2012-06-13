import types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import Time, DictCache
from DIRAC.Core.DISET.RPCClient import RPCClient

class OAToken( object ):

  class DBHold:

    def __init__( self ):
      self.checked = False
      self.reset()

    def reset( self ):
      self.token = False

  __db = DBHold()
  __cache = DictCache()

  _sDisableLocal = False

  class RemoteMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    def __get__( self, obj, type = None ):
      return self.__class__( self.__functor.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      if not funcSelf.localAccess:
        rpc = funcSelf._getTokenStoreClient()
        if kwargs:
          fArgs = ( args, kwargs )
        else:
          fArgs = ( args, )
        fName = self.__functor.__name_
        if fName.find( 'get' ) != 0:
          return getattr( rpc, fName )( fArgs )
        #Cache code
        cKey = "%s( %s )" % ( fName, fArgs )
        data = OAToken.__cache.get( cKey )
        if data:
          return S_OK( data )
        result = getattr( rpc, fName )( fArgs )
        if result[ 'OK' ]:
          OAToken.__cache.add( cKey, 300, result[ 'Value' ] )
        return result
      return self.__functor( *args, **kwargs )

  def __init__( self, forceLocal = False, getRPCFunctor = False ):
    self.__forceLocal = forceLocal
    if getRPCFunctor:
      self.__getRPCFunctor = getRPCFunctor
    else:
      self.__getRPCFunctor = RPCClient
    #Init DB if there
    if not OAToken.__db.checked:
      OAToken.__db.checked = True
      for varName, dbName in ( ( 'token', 'OATokensDB' ), ):
        try:
          dbImp = "RESTDIRAC.RESTSystem.DB.%s" % dbName
          dbMod = __import__( dbImp, fromlist = [ dbImp ] )
          dbClass = getattr( dbMod, dbName )
          dbInstance = dbClass()
          setattr( OAToken.__db, varName, dbInstance )
          result = dbInstance._getConnection()
          if not result[ 'OK' ]:
            gLogger.warn( "Could not connect to %s (%s). Resorting to RPC" % ( dbName, result[ 'Message' ] ) )
            OAToken.__db.reset()
            break
          else:
            result[ 'Value' ].close()
        except RuntimeError:
          OAToken.__db.reset()
          break
        except ImportError:
          OAToken.__db.reset()
          break

  @property
  def localAccess( self ):
    if OAToken._sDisableLocal:
      return False
    if OAToken.__db.job or self.__forceLocal:
      return True
    return False

  def __getDB( self ):
    return OAToken.__db.token

  def _getTokenStoreClient( self ):
    return self.__getRPCFunctor( "WorkloadManagement/OATokenStore" )


  #Client creation
  @RemoteMethod
  def generateClientPair( self, name, url, redirect, icon ):
    return self.__getDB().generateClientPair( name, url, redirect, icon )

  @RemoteMethod
  def getClientDataByID( self, clientid ):
    return self.__getDB().getClientData
