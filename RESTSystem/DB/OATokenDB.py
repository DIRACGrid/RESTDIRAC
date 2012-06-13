########################################################################
# $HeadURL$
########################################################################
""" 
"""

__RCSID__ = "$Id$"

import time
import sys
import random
try:
  import hashlib as md5
except:
  import md5
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

class OATokenDB( DB ):

  __tables = {}

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'OATokenDB', 'REST/OATokenDB', maxQueueSize )
    random.seed()
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesToCreate = {}


    if 'OA_Client' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Client' ] = { 'Fields' : { 'ClientID': 'CHAR(32) NOT NULL UNIQUE',
                                                          'Secret': 'CHAR(40) NOT NULL UNIQUE',
                                                          'Name': 'VARCHAR(64) NOT NULL UNIQUE',
                                                          'URL': 'VARCHAR(128) NOT NULL',
                                                          'Redirect': 'VARCHAR(128) NOT NULL',
                                                          'Icon': 'VARCHAR(128) NOT NULL'
                                                        },
                                             'PrimaryKey': 'ClientID' 
                                           }

    if 'OA_Code' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Code' ] = { 'Fields' : { 'Code': 'CHAR(40) NOT NULL UNIQUE',
                                                        'ClientID': 'CHAR(32) NOT NULL',
                                                        'User': 'VARCHAR(16) NOT NULL',
                                                        'Group': 'VARCHAR(16) NOT NULL',
                                                        'Type' : 'VARCHAR(8) NOT NULL',
                                                        'Scope': 'VARCHAR(128)',
                                                        'State': 'VARCHAR(32)',
                                                        'RedirectURI': 'VARCHAR(128)',
                                                        'Expiration': 'DATETIME NOT NULL',
                                                        'Used': 'TINYINT(1) NOT NULL DEFAULT 0'
                                                     },
                                           'PrimaryKey' : 'Code'
                                        }

    if 'OA_Token' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Token' ] = { 'Fields' : { 'Token' : 'CHAR(40) NOT NULL UNIQUE',
                                                         'Code' : 'CHAR(40)',
                                                         'ClientID' : 'CHAR(32)',
                                                         'User': 'VARCHAR(16) NOT NULL',
                                                         'Group': 'VARCHAR(16) NOT NULL',
                                                         'Scope': 'VARCHAR(128)',
                                                         'Expiration': 'DATETIME NOT NULL',
                                                         'Class' : 'ENUM( "Access", "Refresh" ) NOT NULL',
                                                         'Type' : 'ENUM( "Bearer", "Mac" ) NOT NULL'
                                                       },
                                             'PrimaryKey' : 'Token'
                                          }

    for key in OATokenDB.__tables:
      if key not in tablesInDB:
        tablesToCreate[ key ] = OATokenDB.__tables[ key ]


    return self._createTables( tablesToCreate )

  def __extract( self, table, condDict = None, cleanExpired = True ):
    tableData = OATokenDB.__tables[ table ]
    fields = tableData[ 'Fields' ].keys()
    if cleanExpired:
      timeStamp = "Expiration"
      older = "UTC_TIMESTAMP()"
    else:
      timeStamp = None
      older = None
    result = self.getFields( table, fields, condDict, older = older, timeStamp = timeStamp )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    objs = []
    primaryKey = tableData[ 'PrimaryKey' ]
    for cData in data:
      objData = {}
      for iP in range( len( fields ) ):
        objData[ fields[ iP ] ] = data[ iP ]
      objs[ objData[ primaryKey ] ] = objData
    return S_OK( objs )


  #############################
  #
  # Clients
  #
  #############################

  def generateClientPair( self, name, url, redirect, icon ):
    clientid = hashlib.md5( "%s|%s|%s" % ( name, time.time(), random.random() ) )
    secret = hashlib.sha1( "%s|%s|%s|%s|%s|%s" % ( name, url, redirect, icon, time.time(), random.random() ) ).hexdigest()
    result = self.insertFields( 'OA_Client', ( 'ClientID', 'Secret', 'Name', 'URL', 'Redirect', 'Icon' ),
                                             ( clientid, secret, name, url, redirect, icon ) )
    if not result[ 'OK' ]:
      if result[ 'Message' ].find( "Duplicate entry" ):
        return S_ERROR( "A client with name %s already exists" % name )
      return result
    consData = { 'ClientID': clientid,
                 'Secret' : secret,
                 'Name' : name,
                 'URL': url,
                 'Redirect' : redirect,
                 'Icon' : icon }
    return S_OK( consData )

  def getClientDataByID( self, clientid ):
    result = self.getClientsData( { 'ClientID': clientid } )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "Unknown client" )
    return S_OK( data[ data.keys()[0] ] )

  def getClientDataByName( self, name ):
    result = self.getClientsData( { 'Name': name } )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "Unknown client" )
    return S_OK( data[ data.keys()[0] ] )

  def getClientsData( self, condDict = None ):
    return self._extract( 'OA_Client', condDict )

  def deleteClientByID( self, clientid ):
    self.__deleteClient( { 'ClientID' : clientid } )
  
  def deleteClientByName( self, name ):
    self.__deleteClient( { 'Name' : name } )
  
  def __deleteClient( self, condDict ):
    totalDeleted = 0
    for table in ( "OA_Client", "OA_Code", "OA_Token" ):
      result = self.deleteEntries( table, condDict )
      if not result[ 'OK' ]:
        return result
      totalDeleted += result[ 'Value' ]
    return S_OK( totalDeleted )

  #############################
  #
  # Code requests
  #
  #############################

  def generateCode( self, clientid, type, user, group, redirect = "", scope = "", state = "" ):
    result = self.getClientDataByID( clientid )
    if not result[ 'OK' ]:
      return result
    consData = result[ 'Value' ]
    if not redirect:
      if consData[ 'redirect' ]:
        redirect = consData[ 'redirect' ]
      else:
        return S_ERROR( "Neither client nor request have a redirect url defined" )
    elif consData[ 'redirect' ]:
      oC = consData[ 'redirect' ]
      if oC.find( redirect ) > 0:
        redirect = oC
      else:
        return S_ERROR( "Invalid redirect url" )

    inFields = [ "Code", "ClientID", "User", "Group", "Type", "Expiration" ]
    inValues = [ "", clientid, user, group, type, "TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % 600 ]
    if scope:
      inFields.append( "Scope" )
      inValues.append( scope )
    if state:
      inFields.append( "State" )
      inValues.append( state )
    while True:
      code = hashlib.sha1( "%s|%s|%s|%s" % ( clientid, type, time.time(), random.random() ) ).hexdigest()
      inValues[ 0 ] = code
      result = self.insertFields( "OA_Request", inFields, inValues )
      if not result[ 'OK' ]:
        if result[ 'Message' ].find( "Duplicate entry" ):
          continue
        return result
      break

    consData[ 'code' ] = code
    consData[ 'redirect' ] = redirect
    return S_OK( consData )


  def getCodeData( self, code ):
    condDict = { 'Code': code }
    result = self.__extract( 'OA_Code', { 'Code' : code } )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ] 
    if not data:
      return S_OK( {} )
    return S_OK( data[ data.keys()[0] ] )

  def deleteCode( self, code ):
    totalDeleted = 0
    for tableName in ( 'OA_Code', 'OA_Token' ):
      result = self.deleteEntries( 'OA_Code', { 'Code': code } )
      if not result[ 'OK' ]:
        return result
      totalDeleted += result[ 'Value' ]
    return S_OK( totalDeleted )

  #############################
  #
  #     Token
  #
  #############################

  def generateToken( self, clientid, code ):
    result = self.__extract( 'OA_Code', { 'ClientID': clientid, 'Code': code } )
    if not result[ 'OK' ]:
      return result
    codeData = result[ 'Value' ]
    if not codeData:
      return S_ERROR( "Code-Client combination is unknown" )
    if codeData[ 'Used' ]:
      self.deleteCode( code )
      return S_ERROR( "Code has already been used! Invalidating all related tokens" )
    result = self.updateFields( 'OA_Code', [ 'Used' ], [ 1 ], { 'Code': code, 'ClientID': clientID, 
                                                                'Used' : 0} )
    if not result[ 'OK' ]:
      return result
    if result[ 'Value' ] == 0:
      self.deleteCode( code )
      return S_ERROR( "Code has already been used! Invalidating all related tokens" )

    tokenClass = [ 'Access' ]
    if codeData[ 'Type' ] == 'code':
      tokenClass.append( 'Refresh' )

    inKeys = [ 'Code', 'ClientID', 'User', 'Group', 'Scope', 'Type', 'Token', 'Class', 'Expiration' ]
    inValues = []
    for ik in inKeys:
      if ik in codeData:
        inValues.append( codeData[ ik ] )
      else:
        inValues.append( "" )

    tokens = { 'token_type' : bearer }
    for tClass in tokenClass:
      token = hexdigest.sha1( str( ( clientid, code, tType, time.time(), random.random() ) ) ).hexdigest()
      inValues[-4] = 'Bearer' 
      inValues[-3] = token 
      inValues[-2] = tClass
      lifetime = 86400 * 365
      if tClass == 'Access':
        lifetime = 86400
        tokens[ 'expires_in' ] = lifetime
      inValues[-1] = 'TIMESTAMPADD( SECONDS, %s, UTC_TIMESTAMP() )' % lifetime
      result = self.insertValues( 'OA_Token', inFields, inValues )
      if not result[ 'OK' ]:
        return result
      tokens[ '%_token' % tClass.lower() ] = token

    return S_OK( tokens )


  def __generateToken( self, userId, consumerKey, lifeTime, retries = 5 ):
    token = md5.md5( "%s|%s|%s|%s|%s" % ( userId, type, consumerKey, time.time(), random.random() ) ).hexdigest()
    secret = md5.md5( "%s|%s|%s|%s" % ( userId, consumerKey, time.time(), random.random() ) ).hexdigest()
    if len( consumerKey ) > 64 or len( consumerKey ) < 5:
      return S_ERROR( "Consumer key doesn't have a correct size" )
    result = self._escapeString( consumerKey )
    if not result[ 'OK' ]:
      return result
    sqlConsumerKey = result[ 'Value' ]

    sqlFields = "( Token, Secret, ConsumerKey, UserId, ExpirationTime )"
    sqlValues = [ "'%s'" % token, "'%s'" % secret, sqlConsumerKey, "%d" % userId,
                 "TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % lifeTime ]
    sqlIn = "INSERT INTO `OA_Tokens` %s VALUES ( %s )" % ( sqlFields, ",".join( sqlValues ) )
    result = self._update( sqlIn )
    if not result[ 'OK' ]:
      if result[ 'Message' ].find( "Duplicate key" ) > -1 and retries > 0 :
        return self.__generateToken( userId, consumerKey, lifeTime, retries - 1 )
      return result
    return S_OK( ( token, secret ) )

  def getTokenData( self, consumerKey, token ):
    result = self._escapeString( token )
    if not result[ 'OK' ]:
      self.logger.error( result[ 'Value' ] )
      return result
    sqlToken = result[ 'Value' ]

    result = self._escapeString( consumerKey )
    if not result[ 'OK' ]:
      self.logger.error( result[ 'Value' ] )
      return result
    sqlConsumerKey = result[ 'Value' ]

    sqlCond = [ "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" ]
    sqlCond.append( "Token=%s" % sqlToken )
    sqlCond.append( "ConsumerKey=%s" % sqlConsumerKey )
    sqlSel = "SELECT Secret, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), UserId FROM `OA_Tokens` WHERE %s" % " AND ".join( sqlCond )
    result = self._query( sqlSel )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) < 1 or len( data[0] ) < 1:
      return S_ERROR( "Unknown token" )
    tokenData = { 'token' : token,
                  'secret' : data[0][0],
                  'lifeTime' : data[0][1] }
    result = self.__getUserAndGroup( data[0][2] )
    if not result[ 'OK' ]:
      return result
    tokenData[ 'userDN'] = result['Value' ][0]
    tokenData[ 'userGroup'] = result['Value' ][1]
    return S_OK( tokenData )

  def getTokens( self, condDict = {} ):
    sqlTables = [ '`OA_Tokens` t', '`CredDB_Identities` i' ]
    sqlCond = [ "t.UserId = i.Id"]

    fields = [ 'Token', 'Secret', 'ConsumerKey', 'UserDN', 'UserGroup' ]
    sqlFields = []
    for field in fields:
      if field in ( 'UserDN', 'UserGroup' ):
        sqlField = "i.%s" % field
      else:
        sqlField = "t.%s" % field
      sqlFields.append( sqlField )
      if field in condDict:
        if type( condDict[ field ] ) not in ( types.ListType, types.TupleType ):
          condDict[ field ] = [ str( condDict[ field ] ) ]
        sqlValues = [ self._escapeString( val )[ 'Value' ] for val in condDict[ field ] ]
        if len( sqlValues ) == 1:
          sqlCond.append( "%s = %s" % ( sqlField, sqlValues[0] ) )
        else:
          sqlCond.append( "%s in ( %s )" % ( sqlField, ",".join( sqlValues[0] ) ) )

    sqlFields.append( "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime )" )
    fields.append( "LifeTime" )
    sqlCmd = "SELECT %s FROM %s WHERE %s" % ( ", ". join( sqlFields ), ", ".join( sqlTables ), " AND ".join( sqlCond ) )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( { 'Parameters' : fields, 'Records' : result[ 'Value' ] } )

  def revokeUserToken( self, userDN, userGroup, token ):
    result = self.getIdentityId( userDN, userGroup )
    if not result[ 'OK' ]:
      self.logger.error( result[ 'Value' ] )
      return result
    userId = result[ 'Value' ]
    return self.revokeToken( token, userId )

  def revokeToken( self, token, userId = -1 ):
    result = self._escapeString( token )
    if not result[ 'OK' ]:
      self.logger.error( result[ 'Value' ] )
      return result
    sqlToken = result[ 'Value' ]
    sqlCond = [ "Token=%s" % sqlToken ]
    try:
      userId = int( userId )
    except ValueError:
      return S_ERROR( "userId has to be an integer" )
    if userId > -1:
      sqlCond.append( "userId = %d" % userId )
    sqlDel = "DELETE FROM `OA_Tokens` WHERE %s" % " AND ".join( sqlCond )
    return self._update( sqlDel )



  def cleanExpired( self, minLifeTime = 0 ):
    try:
      minLifeTime = int( minLifeTime )
    except ValueError:
      return S_ERROR( "minLifeTime has to be an integer" )
    totalCleaned = 0
    for table in ( "OA_Tokens", "OA_Verifier" ):
      sqlDel = "DELETE FROM `%s` WHERE TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %d" % minLifeTime
      result = self._update( sqlDel )
      if not result[ 'OK' ]:
        return result
      totalCleaned += result[ 'Value' ]
    return S_OK( totalCleaned )



