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
      if funcSelf.localAccess:
        return self.__functor( *args, **kwargs )
      rpc = funcSelf._getTokenStoreClient()
      if kwargs:
        fArgs = ( args, kwargs )
      else:
        fArgs = ( args, )
      fName = self.__functor.__name__
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

  def __init__( self, forceLocal = False, getRPCFunctor = False ):
    self.__forceLocal = forceLocal
    if getRPCFunctor:
      self.__getRPCFunctor = getRPCFunctor
    else:
      self.__getRPCFunctor = RPCClient
    #Init DB if there
    if not OAToken.__db.checked:
      OAToken.__db.checked = True
      for varName, dbName in ( ( 'token', 'OATokenDB' ), ):
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
          if self.__forceLocal:
            raise
          OAToken.__db.reset()
          break
        except ImportError:
          if self.__forceLocal:
            raise
          OAToken.__db.reset()
          break

  @property
  def localAccess( self ):
    if OAToken._sDisableLocal:
      return False
    if OAToken.__db.token or self.__forceLocal:
      return True
    return False

  def __getDB( self ):
    return OAToken.__db.token

  def _getTokenStoreClient( self ):
    return self.__getRPCFunctor( "REST/OATokenStore" )


  #Client creation
  @RemoteMethod
  def generateClientPair( self, name, url, redirect, icon ):
    return self.__getDB().generateClientPair( name, url, redirect, icon )

  @RemoteMethod
  def getClientDataByID( self, clientid ):
    return self.__getDB().getClientDataByID( clientid )
  
  @RemoteMethod
  def getClientDataByName( self, name ):
    return self.__getDB().getClientDataByName( name )

  @RemoteMethod
  def getClientsData( self, condDict = None ):
    return self.__getDB().getClientsData( condDict )

  @RemoteMethod
  def deleteClientByID( self, cid ):
    return self.__getDB().deleteClientByID( cid )

  @RemoteMethod
  def deleteClientByName( self, name ):
    return self.__getDB().deleteClientByName( name )


  #Codes
  @RemoteMethod
  def generateCode( self, cid, type, user, group, redirect = "", scope = "", state = "" ):
    return self.__getDB().generateCode( cid, type, user, group, redirect, scope, state )

  @RemoteMethod
  def getCodeData( self, code ):
    return self.__getDB().getCodeData( code )

  @RemoteMethod
  def deleteCode( self, code ):
    return self.__getDB().deleteCode( code )


  #Tokens
  @RemoteMethod
  def generateTokenFromCode( self, cid, code ):
    return self.__getDB().generateTokenFromCode( cid, code )

