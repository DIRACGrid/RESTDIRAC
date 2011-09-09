import sys
import time

from WebAPIDIRAC.WebAPISystem.DB.CredentialsDB import CredentialsDB
from WebAPIDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient


def testCredObj( credClient ):
  userDN = "/dummyDB"
  userGroup = "dummyGroup"
  consumerKey = "dummyConsumer"
  for tType in ( "request", "access" ):
    print "Trying token type: %s" % tType
    result = credClient.generateToken( userDN, userGroup, consumerKey, tokenType = tType )
    if not result[ 'OK' ]:
      print "[ERR] %s" % result['Message']
      return False
    tokenPair = result[ 'Value' ]
    result = credClient.getSecret( userDN, userGroup, consumerKey, tokenPair[0], tokenType = tType )
    if not result[ 'OK' ]:
      print "[ERR] %s" % result['Message']
      return False
    secret = result[ 'Value' ]
    if secret != tokenPair[1]:
      print "[ERR] SECRET IS DIFFERENT!!"
    print "Token is OK. Revoking token with wrong user..."
    result = credClient.revokeUserToken( "no", "no", tokenPair[0] )
    if not result[ 'OK' ]:
      print "[ERR] %s" % result['Message']
      return False
    if result[ 'Value' ] != 0:
      print "[ERR] %d tokens were revoked" % result[ 'Value' ]
      return False
    print "Revoking token with user"
    result = credClient.revokeUserToken( userDN, userGroup, tokenPair[0] )
    if not result[ 'OK' ]:
      print "[ERR] %s" % result['Message']
      return False
    if result[ 'Value' ] != 1:
      print "[ERR] %d tokens were revoked" % result[ 'Value' ]
      return False
    print "Token was revoked"
  print "Cleaning expired tokens"
  result = credClient.cleanExpired()
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    return False
  print "Generating 1 sec lifetime token"
  result = credClient.generateToken( userDN, userGroup, consumerKey, tokenType = tType, lifeTime = 1 )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    return False
  print "Sleeping 2 sec"
  time.sleep( 2 )
  print "Cleaning expired tokens"
  result = credClient.cleanExpired()
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    return False
  if result[ 'Value' ] < 1:
    print "[ERR] No tokens were cleaned"
    return False
  print "%s tokens were cleaned" % result[ 'Value' ]
  print "Requesting verifier"
  result = credClient.generateVerifier( userDN, userGroup, consumerKey )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    return False
  verifier = result[ 'Value' ]
  print "Trying to validate with different consumer"
  result = credClient.validateVerifier( userDN, userGroup, "ASD%s" % consumerKey, verifier )
  if not result[ 'OK' ]:
    print "Not validated with: %s" % result[ 'Message' ]
  else:
    print "[ERR] Validated invalid verifier"
    return False
  print "Validating it"
  result = credClient.validateVerifier( userDN, userGroup, consumerKey, verifier )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    return False
  print "ALL OK"
  return True

if __name__ == "__main__":
  for credObj in ( CredentialsClient(), CredentialsDB() ):
    print "====== TESTING %s ======" % credObj
    if not testCredObj( credObj ):
      print "EXITING"
      sys.exit( 1 )
