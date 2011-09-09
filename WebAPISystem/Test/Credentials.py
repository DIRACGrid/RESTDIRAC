import sys
import time

from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient

def checkRes( result ):
  if not result[ 'OK' ]:
    raise RuntimeError( result[ 'Message' ] )
  return result[ 'Value' ]

def testCredObj( credClient ):
  userDN = "/dummyDB"
  userGroup = "dummyGroup"
  consumerKey = "dummyConsumer"
  consumerSecret = ""
  print "Checking if the consumer exists"
  result = credClient.getConsumerSecret( consumerKey )
  if not result[ 'OK' ]:
    print "Creating consumer"
    result = credClient.generateConsumerPair( consumerKey )
    consumerSecret = checkRes( result )[1]
  consumerSecret = result[ 'Value' ][1]
  print " -- Testing Requests"
  print "Generating request"
  requestPair = checkRes( credClient.generateRequest( consumerKey ) )
  print "Verifying request"
  secret = checkRes( credClient.getRequestSecret( consumerKey, requestPair[0] ) )
  if secret != requestPair[1]:
    raise RuntimeError( "Request secrets are different" )
  print "Deleting request"
  if 1 != checkRes( credClient.deleteRequest( requestPair[0] ) ):
    raise RuntimeError( "Didn't delete the request" )
  print " -- Testing tokens"
  print "Generating a new request"
  request = checkRes( credClient.generateRequest( consumerKey ) )[0]
  tokenPair = checkRes( credClient.generateToken( userDN, userGroup, consumerKey, request ) )
  if tokenPair[1] != checkRes( credClient.getTokenSecret( userDN, userGroup, consumerKey, tokenPair[0] ) ):
    raise RuntimeError( "SECRET IS DIFFERENT!!" )
  print "Token is OK. Revoking token with wrong user..."
  revoked = checkRes( credClient.revokeUserToken( "no", "no", tokenPair[0] ) )
  if 0 != revoked:
    raise RuntimeError( "%d tokens were revoked" % revoked )
  print "Revoking token with user"
  revoked = checkRes( credClient.revokeUserToken( userDN, userGroup, tokenPair[0] ) )
  if 1 != revoked:
    raise RuntimeError( "%d tokens were revoked" % revoked )
  print "Token was revoked"
  print " -- Testing cleaning"
  print "Cleaning expired tokens"
  checkRes( credClient.cleanExpired() )
  print "Generating 1 sec lifetime token"
  request = checkRes( credClient.generateRequest( consumerKey ) )[0]
  checkRes( credClient.generateToken( userDN, userGroup, consumerKey, request, lifeTime = 1 ) )
  print "Sleeping 2 sec"
  time.sleep( 2 )
  print "Cleaning expired tokens"
  cleaned = checkRes( credClient.cleanExpired() )
  if cleaned == 0:
    raise RuntimeError( "No tokens were cleaned" )
  print "%s tokens were cleaned" % cleaned
  print " -- Testing Verifiers"
  print "Requesting verifier"
  verifier = checkRes( credClient.generateVerifier( userDN, userGroup, consumerKey ) )
  print "Trying to validate with different consumer"
  result = credClient.validateVerifier( userDN, userGroup, "ASD%s" % consumerKey, verifier )
  if result[ 'OK' ]:
    raise RuntimeError( "Validated invalid verifier" )
  else:
    print "Not validated with: %s" % result[ 'Message' ]
  print "Validating it"
  checkRes( credClient.validateVerifier( userDN, userGroup, consumerKey, verifier ) )
  print " -- Testing consumers"
  print "Getting token"
  request = checkRes( credClient.generateRequest( consumerKey ) )[0]
  tokenPair = checkRes( credClient.generateToken( userDN, userGroup, consumerKey, request ) )
  print "Requesting verifier"
  verifier = checkRes( credClient.generateVerifier( userDN, userGroup, consumerKey ) )
  print "Deleting consumer"
  cleaned = checkRes( credClient.deleteConsumer( consumerKey ) )
  if cleaned < 3:
    raise RuntimeError( "Deleted less than three objects: %d" % cleaned )
  print "%d objects were deleted" % cleaned
  print "Trying to retrieve token"
  result = credClient.getTokenSecret( userDN, userGroup, consumerKey, tokenPair[0] )
  if result[ 'OK' ]:
    raise RuntimeError( "Token could be retrieved!" )
  print "Token was deleted :)"
  print "Trying to retrieve verifier"
  result = credClient.validateVerifier( userDN, userGroup, consumerKey, verifier )
  if result[ 'OK' ]:
    raise RuntimeError( "Verifier could be retrieved!" )
  print "Verifier was deleted :)"
  print "ALL OK"

if __name__ == "__main__":
  for credObj in ( CredentialsDB(), CredentialsClient() ):
    print "====== TESTING %s ======" % credObj
    testCredObj( credObj )
