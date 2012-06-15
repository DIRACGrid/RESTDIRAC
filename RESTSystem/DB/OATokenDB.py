########################################################################
# $HeadURL$
########################################################################
""" 
"""

__RCSID__ = "$Id$"

import time
import sys
import random
import hashlib
import base64
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
      OATokenDB.__tables[ 'OA_Client' ] = { 'Fields' : { 'ClientID': 'CHAR(28) NOT NULL UNIQUE',
                                                         'Secret': 'CHAR(28) NOT NULL UNIQUE',
                                                         'Name': 'VARCHAR(64) NOT NULL UNIQUE',
                                                         'URL': 'VARCHAR(128) NOT NULL',
                                                         'Redirect': 'VARCHAR(128) NOT NULL',
                                                         'Icon': 'VARCHAR(128) NOT NULL'
                                                       },
                                            'PrimaryKey': 'ClientID' 
                                          }

    if 'OA_Code' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Code' ] = { 'Fields' : { 'Code': 'CHAR(28) NOT NULL UNIQUE',
                                                       'ClientID': 'CHAR(28) NOT NULL',
                                                       'User': 'VARCHAR(16) NOT NULL',
                                                       'Group': 'VARCHAR(16) NOT NULL',
                                                       'Scope': 'VARCHAR(128)',
                                                       'State': 'VARCHAR(32)',
                                                       'RedirectURI': 'VARCHAR(128)',
                                                       'Expiration': 'DATETIME NOT NULL',
                                                       'Used': 'TINYINT(1) NOT NULL DEFAULT 0'
                                                    },
                                          'PrimaryKey' : 'Code'
                                       }

    if 'OA_Token' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Token' ] = { 'Fields' : { 'Token' : 'CHAR(28) NOT NULL UNIQUE',
                                                         'Code' : 'CHAR(28)',
                                                         'Secret' : 'CHAR(28)',
                                                         'ClientID' : 'CHAR(28)',
                                                         'User': 'VARCHAR(16) NOT NULL',
                                                         'Group': 'VARCHAR(16) NOT NULL',
                                                         'Scope': 'VARCHAR(128)',
                                                         'Expiration': 'DATETIME NOT NULL',
                                                         'Class' : 'ENUM( "Access", "Refresh" ) NOT NULL',
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
    objs = {}
    primaryKey = tableData[ 'PrimaryKey' ]
    for cData in data:
      objData = {}
      for iP in range( len( fields ) ):
        objData[ fields[ iP ] ] = cData[ iP ]
      objs[ objData[ primaryKey ] ] = objData
    return S_OK( objs )

  def __hash( self, data ):
    """ Create a token based on the data
    """
    return base64.urlsafe_b64encode( hashlib.sha1( "%s|%s|%s" % ( data, time.time(), random.random() ) ).digest() )

  #############################
  #
  # Clients
  #
  #############################

  def generateClientPair( self, name, url, redirect, icon ):
    cid = self.__hash( "%s|%s|%s" % ( name, url, icon ) ) 
    secret = self.__hash( "%s|%s|%s" % ( name, url, cid ) ) 
    result = self.insertFields( 'OA_Client', ( 'ClientID', 'Secret', 'Name', 'URL', 'Redirect', 'Icon' ),
                                             ( cid, secret, name, url, redirect, icon ) )
    if not result[ 'OK' ]:
      if result[ 'Message' ].find( "Duplicate entry" ):
        return S_ERROR( "A client with name %s already exists" % name )
      return result
    consData = { 'ClientID': cid,
                 'Secret' : secret,
                 'Name' : name,
                 'URL': url,
                 'Redirect' : redirect,
                 'Icon' : icon }
    return S_OK( consData )

  def getClientDataByID( self, cid ):
    result = self.getClientsData( { 'ClientID': cid } )
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
    return self.__extract( 'OA_Client', condDict, cleanExpired = False )

  def deleteClientByID( self, cid ):
    return self.__deleteClient( { 'ClientID' : cid } )
  
  def deleteClientByName( self, name ):
    return self.__deleteClient( { 'Name' : name } )
  
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

  def generateCode( self, cid, type, user, group, redirect = "", scope = "", state = "" ):
    result = self.getClientDataByID( cid )
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
    inValues = [ "", cid, user, group, type, "TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % 600 ]
    if scope:
      inFields.append( "Scope" )
      inValues.append( scope )
    if state:
      inFields.append( "State" )
      inValues.append( state )
    while True:
      code = self.__hash( "%s|%s" % ( cid, type ) )
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

  def generateTokenFromCode( self, cid, code, secret = False ):
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

    inData = {}
    for k in ( 'Code', 'ClientID', 'User', 'Group', 'Scope' ):
      inData[ k ] = codeData[ k ]

    tokens = {}
    for tClass in tokenClass:
      token = self.__hash( str( ( cid, code, tClass ) ) )
      tData = {}
      for k, v in ( ( 'Token', token ), ( 'Class', tClass ) ):
        tData[ k ] = v
        inData[ k ] = v
      if secret:
        secret = self.__hash( str( codeData, token ) )
        inData[ 'Secret' ] = secret
        tData[ 'Secret' ] = secret
      lifetime = 86400 * 365
      if tClass == 'Access':
        lifetime = 86400
      inData[ 'Expiration' ] = 'TIMESTAMPADD( SECONDS, %s, UTC_TIMESTAMP() )' % lifetime
      tData[ 'LifeTime' ] = lifetime
      result = self.insertValues( 'OA_Token', inDict = inData )
      if not result[ 'OK' ]:
        return result
      tokens[ tClass ] = tData

    return S_OK( tokens )

  def generateRawToken( self, user, group, scope = "", secret = False ):

    tokenClass = [ 'Access' ]
    if codeData[ 'Type' ] == 'code':
      tokenClass.append( 'Refresh' )

    inData = { 'User' : user, 'Group' : 'group' }
    if scope:
      inData[ 'Scope' ] = scope

    tokens = {}
    for tClass in tokenClass:
      token = self.__hash( str( ( user, group, scope ) ) )
      tData = {}
      for k, v in ( ( 'Token', token ), ( 'Class', tClass ) ):
        tData[ k ] = v
        inData[ k ] = v
      if secret:
        secret = self.__hash( str( user, group, token ) )
        inData[ 'Secret' ] = secret
        tData[ 'Secret' ] = secret
      lifetime = 86400 * 365
      if tClass == 'Access':
        lifetime = 86400
      inData[ 'Expiration' ] = 'TIMESTAMPADD( SECONDS, %s, UTC_TIMESTAMP() )' % lifetime
      tData[ 'LifeTime' ] = lifetime
      result = self.insertValues( 'OA_Token', inDict = inData )
      if not result[ 'OK' ]:
        return result
      tokens[ tClass ] = tData

    return S_OK( tokens )


