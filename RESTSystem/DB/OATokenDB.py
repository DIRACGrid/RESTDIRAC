########################################################################
# $HeadURL$
########################################################################
"""
"""

__RCSID__ = "$Id$"

import time
import types
import sys
import random
import hashlib
import base64
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

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
      OATokenDB.__tables[ 'OA_Client' ] = { 'Fields' : { 'ClientID': 'CHAR(29) NOT NULL UNIQUE',
                                                         'Name': 'VARCHAR(64) NOT NULL UNIQUE',
                                                         'URL': 'VARCHAR(128) NOT NULL',
                                                         'Redirect': 'VARCHAR(128) NOT NULL',
                                                         'Icon': 'VARCHAR(128) NOT NULL'
                                                       },
                                            'PrimaryKey': 'ClientID'
                                          }

    if 'OA_Code' not in OATokenDB.__tables:
      OATokenDB.__tables[ 'OA_Code' ] = { 'Fields' : { 'Code': 'CHAR(29) NOT NULL UNIQUE',
                                                       'ClientID': 'CHAR(28) NOT NULL',
                                                       'UserDN': 'VARCHAR(128) DEFAULT NULL',
                                                       'UserGroup': 'VARCHAR(16) DEFAULT NULL',
                                                       'LifeTime' : 'INT UNSIGNED NOT NULL',
                                                       'Scope': 'VARCHAR(128) DEFAULT NULL',
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
                                                         'UserName': 'VARCHAR(16) NOT NULL',
                                                         'UserDN': 'VARCHAR(128) NOT NULL',
                                                         'UserGroup': 'VARCHAR(16) NOT NULL',
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

  def __extract( self, table, condDict = None, cleanExpired = True, single = False ):
    tableData = OATokenDB.__tables[ table ]
    fields = tableData[ 'Fields' ].keys()
    if cleanExpired:
      timeStamp = "Expiration"
      date = "UTC_TIMESTAMP()"
    else:
      timeStamp = None
      date = None
    if type( condDict ) != types.DictType:
      condDict = False
    else:
      filteredDict = {}
      for k in list( condDict.keys() ):
        if k in fields:
          filteredDict[ k ] = condDict[ k ]
    result = self.getFields( table, fields, filteredDict, newer = date, timeStamp = timeStamp )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    objs = {}
    primaryKey = tableData[ 'PrimaryKey' ]
    for cData in data:
      objData = {}
      for iP in range( len( fields ) ):
        objData[ fields[ iP ] ] = cData[ iP ]
      if single:
        return S_OK( objData )
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

  def registerClient( self, name, redirect, url, icon ):
    cid = self.__hash( "%s|%s|%s" % ( name, url, icon ) )
    result = self.insertFields( 'OA_Client', ( 'ClientID', 'Name', 'URL', 'Redirect', 'Icon' ),
                                             ( cid, name, url, redirect, icon ) )
    if not result[ 'OK' ]:
      if result[ 'Message' ].find( "Duplicate entry" ):
        return S_ERROR( "A client with name %s already exists" % name )
      return result
    consData = { 'ClientID': cid,
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

  def generateCode( self, cid, userDN, userGroup, lifeTime, scope = "" ):
    result = self.getClientDataByID( cid )
    if not result[ 'OK' ]:
      return result
    consData = result[ 'Value' ]

    inData = { 'ClientID' : cid,
               'Expiration' : "TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % 600,
               'UserDN' : userDN,
               'UserGroup' : userGroup,
               'LifeTime' : lifeTime }
    if scope:
      inData[ 'Scope' ] = scope
    while True:
      inData[ 'Code' ] = self.__hash( "%s|%s" % ( cid, type ) )
      result = self.insertFields( "OA_Code", inDict = inData )
      if not result[ 'OK' ]:
        if result[ 'Message' ].find( "Duplicate entry" ) > -1:
          continue
        return result
      break

    return S_OK( inData[ 'Code' ] )


  def getCodeData( self, code ):
    condDict = { 'Code': code }
    return self.__extract( 'OA_Code', { 'Code' : code }, single = True )

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

  def generateTokenFromCode( self, cid, code, secret = False, renewable = True ):
    result = self.__extract( 'OA_Code', { 'ClientID': cid, 'Code': code }, single = True )
    if not result[ 'OK' ]:
      return result
    codeData = result[ 'Value' ]
    if not codeData:
      return S_ERROR( "Code-Client combination is unknown" )
    if codeData[ 'Used' ]:
      self.deleteCode( code )
      return S_ERROR( "Code has already been used! Invalidating all related tokens" )
    result = self.updateFields( 'OA_Code', [ 'Used' ], [ 1 ], { 'Code': code, 'ClientID': cid,
                                                                'Used' : 0} )
    if not result[ 'OK' ]:
      return result
    if result[ 'Value' ] == 0:
      self.deleteCode( code )
      return S_ERROR( "Code has already been used! Invalidating all related tokens" )

    return self.generateToken( codeData[ 'UserDN' ], codeData[ 'UserGroup' ], scope = codeData[ 'Scope' ],
                               cid = codeData[ 'ClientID' ], secret = secret, renewable = renewable,
                               code = codeData[ 'Code' ], lifeTime = codeData[ 'LifeTime' ] )

  def generateToken( self, userDN, userGroup, scope = "", cid = False,
                     secret = False, renewable = True, lifeTime = 86400, code = False ):
    tokenClass = [ 'Access' ]
    if renewable:
      tokenClass.append( 'Refresh' )

    inData = { 'UserDN' : userDN, 'UserGroup' : userGroup }
    if scope:
      inData[ 'Scope' ] = scope
    if code:
      inData[ 'Code' ] = code

    result = Registry.getUsernameForDN( userDN )
    if not result[ 'OK' ]:
      return result
    inData[ 'UserName' ] = result[ 'Value' ]

    if cid:
      #If code is given no need to check cid (already checked)
      if not code:
        result = self.getClientDataByID( cid )
        if not result[ 'OK' ]:
          return result

      inData[ 'ClientID' ] = cid

    tokens = {}
    for tClass in tokenClass:
      token = self.__hash( str( ( userDN, userGroup, scope ) ) )
      tData = {}
      for k, v in ( ( 'Token', token ), ( 'Class', tClass ) ):
        tData[ k ] = v
        inData[ k ] = v
      if secret:
        secret = self.__hash( str( userDN, userGroup, token ) )
        inData[ 'Secret' ] = secret
        tData[ 'Secret' ] = secret
      tLifeTime = lifeTime
      if tClass == 'Access':
        tLifeTime = 86400
      inData[ 'Expiration' ] = 'TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )' % tLifeTime
      tData[ 'LifeTime' ] = tLifeTime
      result = self.insertFields( 'OA_Token', inDict = inData )
      if not result[ 'OK' ]:
        return result
      tokens[ tClass ] = tData

    return S_OK( tokens )

  def getTokenData( self, token ):
    return self.__extract( "OA_Token", { 'Token' : token }, single = True )

  def getTokensData( self, condDict ):
    return self.__extract( "OA_Token", condDict )

  def revokeToken( self, token ):
    return self.deleteEntries( "OA_Token", { 'Token' : token } )

  def revokeTokens( self, condDict ):
    return self.deleteEntries( "OA_Token", condDict )

