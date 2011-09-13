import sys
import time

from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient

def checkRes( result ):
  if not result[ 'OK' ]:
    raise RuntimeError( result[ 'Message' ] )
  return result[ 'Value' ]

def testCredObj( credClient ):
  userDN = "dummyDN"
  userGroup = "dummyGroup"
  consumerKey = "testConsumer"
  consumerSecret = ""
  print "Checking if the consumer exists"
  result = credClient.getConsumerData( consumerKey )
  if not result[ 'OK' ]:
    print "Creating consumer"
    result = credClient.generateConsumerPair( "NAME", "CALLBACK", "URL", consumerKey )
    consumerSecret = checkRes( result )[ 'secret' ]
  else:
    consumerSecret = result[ 'Value' ][ 'secret' ]
  print " -- Testing Requests"
  print "Creating consumer without callback"
  checkRes( credClient.generateConsumerPair( "DUMMYTEST", "", "", "nocbkey" ) )
  result = credClient.generateRequest( "nocbkey" )
  if result[ 'OK' ]:
    raise RuntimeError( "Consumer and key don't have a callback and it suceeded" )
  if result[ 'Message' ].find( "Neither" ) != 0:
    raise RuntimeError( result[ 'Message' ] )
  print "Deleting test consumer"
  checkRes( credClient.deleteConsumer( "nocbkey " ) )
  print "Generating request"
  reqData = checkRes( credClient.generateRequest( consumerKey ) )
  print "Verifying request"
  reqTest = checkRes( credClient.getRequestData( reqData[ 'request' ] ) )
  if reqTest != reqData:
    print reqTest
    print reqData
    raise RuntimeError( "Request data are different" )
  print "Deleting request"
  if 1 != checkRes( credClient.deleteRequest( reqData[ 'request' ] ) ):
    raise RuntimeError( "Didn't delete the request" )

  print " -- Testing Verifiers"
  print "Requesting verifier"
  reqData = checkRes( credClient.generateRequest( consumerKey ) )
  verifier = checkRes( credClient.generateVerifier( consumerKey, reqData[ 'request' ], userDN, userGroup ) )
  print "Checking user"
  if ( userDN, userGroup ) != checkRes( credClient.getVerifierUserAndGroup( consumerKey, reqData[ 'request' ], verifier ) ):
    raise RuntimeError( "Users are different for verifier" )
  print "Checking verifier"
  if verifier != checkRes( credClient.getVerifierData( consumerKey, reqData[ 'request' ] ) )[ 'verifier' ]:
    raise RuntimeError( "Different verifier returned" )
  print "Changing lifetime"
  if checkRes( credClient.setVerifierProperties( consumerKey, reqData[ 'request' ], verifier,
                                              userDN, userGroup, 86400 ) ) != 1:
    raise RuntimeError( "Did not modify the verifier" )
  print "Trying to expire with different consumer"
  result = credClient.expireVerifier( "ASD%s" % consumerKey, reqData[ 'request' ], verifier )
  if result[ 'OK' ]:
    raise RuntimeError( "Validated invalid verifier" )
  else:
    print "Not validated with: %s" % result[ 'Message' ]
  print "Expiring it"
  checkRes( credClient.expireVerifier( consumerKey, reqData[ 'request' ], verifier ) )

  print " -- Testing tokens"
  print "Generating a new token"
  reqData = checkRes( credClient.generateRequest( consumerKey ) )
  verifier = checkRes( credClient.generateVerifier( consumerKey, reqData[ 'request' ], userDN, userGroup ) )
  tokenData = checkRes( credClient.generateToken( consumerKey, reqData[ 'request' ], verifier ) )
  if tokenData[ 'secret' ] != checkRes( credClient.getTokenData( consumerKey, tokenData[ 'token' ] ) )[ 'secret' ]:
    raise RuntimeError( "SECRET IS DIFFERENT!!" )
  print "Token is OK. Revoking token with wrong user..."
  revoked = checkRes( credClient.revokeUserToken( "no", "no", tokenData[ 'token' ] ) )
  if 0 != revoked:
    raise RuntimeError( "%d tokens were revoked" % revoked )
  print "Revoking token with user"
  revoked = checkRes( credClient.revokeUserToken( userDN, userGroup, tokenData[ 'token' ] ) )
  if 1 != revoked:
    raise RuntimeError( "%d tokens were revoked" % revoked )
  print "Token was revoked"

  print " -- Testing cleaning"
  print "Cleaning expired tokens"
  checkRes( credClient.cleanExpired() )
  print "Generating 2 sec lifetime token"
  reqData = checkRes( credClient.generateRequest( consumerKey ) )
  verifier = checkRes( credClient.generateVerifier( consumerKey, reqData[ 'request' ], userDN, userGroup, lifeTime = 2 ) )
  tokenData = checkRes( credClient.generateToken( consumerKey, reqData[ 'request' ], verifier ) )
  print "Sleeping 3 sec"
  time.sleep( 3 )
  print "Cleaning expired tokens"
  cleaned = checkRes( credClient.cleanExpired() )
  if cleaned == 0:
    raise RuntimeError( "No tokens were cleaned" )
  print "%s tokens were cleaned" % cleaned

  print " -- Testing consumers"
  print "Getting token"
  reqData = checkRes( credClient.generateRequest( consumerKey ) )
  verifier = checkRes( credClient.generateVerifier( consumerKey, reqData[ 'request' ], userDN, userGroup ) )
  print "Deleting consumer"
  cleaned = checkRes( credClient.deleteConsumer( consumerKey ) )
  if cleaned < 2:
    raise RuntimeError( "Deleted less than three objects: %d" % cleaned )
  print "%d objects were deleted" % cleaned
  print "Trying to retrieve token"
  result = credClient.getTokenData( consumerKey, tokenData[ 'token' ] )
  if result[ 'OK' ]:
    raise RuntimeError( "Token could be retrieved!" )
  print "Token was deleted :)"
  print "Trying to retrieve verifier"
  result = credClient.expireVerifier( consumerKey, reqData[ 'request' ], verifier )
  if result[ 'OK' ]:
    raise RuntimeError( "Verifier could be retrieved!" )
  print "Verifier was deleted :)"
  print "ALL OK"

if __name__ == "__main__":
  #for credObj in ( CredentialsDB(), CredentialsClient() ):
  #for credObj in ( CredentialsDB(), ):
  for credObj in ( CredentialsClient(), ):
    print "====== TESTING %s ======" % credObj
    testCredObj( credObj )
