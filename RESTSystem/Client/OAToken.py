import types
import functools
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import Time, DictCache
from DIRAC.Core.Utilities.Backports import getcallargs
from DIRAC.Core.DISET.RPCClient import RPCClient

__remoteMethods__ = []


def RemoteMethod( method ):

  __remoteMethods__.append( method.__name__ )

  @functools.wraps( method )
  def wrapper( self, *args, **kwargs ):
    if self.localAccess:
      return method( self, *args, **kwargs )
    rpc = self._getTokenStoreClient()
    fName = method.__name__
    return getattr( rpc, fName )( ( args, kwargs ) )

  wrapper.__sneakybastard__ = method
  return wrapper

class Cache( object ):

  __caches = {}

  def __init__( self, cName, atName = False, cacheTime = 300 ):
    if cName not in self.__caches:
      self.__caches[ cName ] = DictCache()
    self.__cache = self.__caches[ cName ]
    self.__atName = atName
    self.__cacheTime = cacheTime

  @classmethod
  def getCache( cls, cName ):
    return cls.__caches[ cName ]

  def __call__( self, method ):

    def wrapped( rSelf, *args, **kwargs ):
      try:
        rMethod = method.__sneakybastard__
      except AttributeError:
        rMethod = method
        pass
      fArgs = getcallargs( rMethod, rSelf, *args, **kwargs )
      if self.__atName:
        cKey = fArgs[ self.__atName ]
      else:
        cKey = tuple( str( fArgs[k] ) for k in sorted( fArgs ) if k != 'self' )
      value = self.__cache.get( cKey )
      if value:
        return value
      value = rMethod( **fArgs )
      if not value[ 'OK' ]:
        return value
      self.__cache.add( cKey, self.__cacheTime, value )
      return value

    return wrapped



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
        except ( ImportError, RuntimeError ):
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
  @Cache( 'client', 'name' )
  @RemoteMethod
  def registerClient( self, name, redirect, url, icon ):
    return self.__getDB().registerClient( name, redirect, url, icon )

  @Cache( 'client' )
  @RemoteMethod
  def getClientDataByID( self, cid ):
    return self.__getDB().getClientDataByID( cid )

  @Cache( 'client' )
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
  def generateCode( self, cid, userDN, userGroup, userSetup, lifeTime, scope = "", redirect = "" ):
    return self.__getDB().generateCode( cid, userDN, userGroup, userSetup, lifeTime, scope, redirect )

  @RemoteMethod
  def getCodeData( self, code ):
    return self.__getDB().getCodeData( code )

  @RemoteMethod
  def deleteCode( self, code ):
    return self.__getDB().deleteCode( code )


  #Tokens
  @RemoteMethod
  def generateTokenFromCode( self, cid, code, redirect = False, secret = False, renewable = True ):
    return self.__getDB().generateTokenFromCode( cid, code, redirect, secret, renewable )

  @RemoteMethod
  def generateToken( self, user, group, setup, scope = "", cid = False, secret = False, renewable = True, lifeTime = 86400 ):
    return self.__getDB().generateToken( user, group, setup, scope, cid, secret, renewable, lifeTime )

  def getCachedToken( self, token ):
    cacheDict = Cache.getCache( 'token' )
    cKey = ( token, )
    value = cacheDict.get( cKey )
    if value:
      return value
    result = self.getTokensData( {} )
    if not result[ 'OK' ]:
      return result
    tokenData = result[ 'Value' ]
    for tokenKey in tokenData:
      cacheDict.add( ( tokenKey, ), 300, S_OK( tokenData[ tokenKey ] ) )
    if token in tokenData:
      return S_OK( tokenData[ token ] )
    return S_ERROR( "Unknown token" )

  @Cache( 'token' )
  @RemoteMethod
  def getTokenData( self, token ):
    return self.__getDB().getTokenData( token )

  @RemoteMethod
  def getTokensData( self, condDict ):
    return self.__getDB().getTokensData( condDict )

  @RemoteMethod
  def revokeToken( self, token ):
    return self.__getDB().revokeToken( token )

  @RemoteMethod
  def revokeTokens( self, condDict ):
    return self.__getDB().revokeTokens( condDict )
