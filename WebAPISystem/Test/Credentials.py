
from OAuthDIRAC.WebAPISystem.Credentials.CredentialsDB import testDB
from OAuthDIRAC.WebAPISystem.Client.CredentialsClient import CredentialsClient

credDB = CredentialsClient()

userDN = "/me"
userGroup = "mygroup"
consumerKey = "ASDAS"
for tType in ( "request", "access" ):
  print "Trying token type: %s" % tType
  result = credDB.generateToken( userDN, userGroup, consumerKey, tokenType = tType )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    sys.exit( 1 )
  tokenPair = result[ 'Value' ]
  result = credDB.getSecret( userDN, userGroup, consumerKey, tokenPair[0], tokenType = tType )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    sys.exit( 1 )
  secret = result[ 'Value' ]
  if secret != tokenPair[1]:
    print "[ERR] SECRET IS DIFFERENT!!"
  print "Token is OK. Revoking token with wrong user..."
  result = credDB.revokeUserToken( "no", "no", tokenPair[0] )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    sys.exit( 1 )
  if result[ 'Value' ] != 0:
    print "[ERR] %d tokens were revoked" % result[ 'Value' ]
    sys.exit( 1 )
  print "Revoking token with user"
  result = credDB.revokeUserToken( userDN, userGroup, tokenPair[0] )
  if not result[ 'OK' ]:
    print "[ERR] %s" % result['Message']
    sys.exit( 1 )
  if result[ 'Value' ] != 1:
    print "[ERR] %d tokens were revoked" % result[ 'Value' ]
    sys.exit( 1 )
  print "Token was revoked"
print "Cleaning expired tokens"
result = credDB.cleanExpired()
if not result[ 'OK' ]:
  print "[ERR] %s" % result['Message']
  sys.exit( 1 )
print "Generating 1 sec lifetime token"
result = credDB.generateToken( userDN, userGroup, consumerKey, tokenType = tType, lifeTime = 1 )
if not result[ 'OK' ]:
  print "[ERR] %s" % result['Message']
  sys.exit( 1 )
print "Sleeping 2 sec"
time.sleep( 2 )
print "Cleaning expired tokens"
result = credDB.cleanExpired()
if not result[ 'OK' ]:
  print "[ERR] %s" % result['Message']
  sys.exit( 1 )
if result[ 'Value' ] < 1:
  print "[ERR] No tokens were cleaned"
  sys.exit( 1 )
print "%s tokens were cleaned" % result[ 'Value' ]
print "Requesting verifier"
result = credDB.generateVerifier( userDN, userGroup, consumerKey )
if not result[ 'OK' ]:
  print "[ERR] %s" % result['Message']
  sys.exit( 1 )
verifier = result[ 'Value' ]
print "Trying to validate with different consumer"
result = credDB.validateVerifier( userDN, userGroup, "ASD%s" % consumerKey, verifier )
if not result[ 'OK' ]:
  print "Not validated with: %s" % result[ 'Message' ]
else:
  print "[ERR] Validated invalid verifier"
  sys.exit( 1 )
print "Validating it"
result = credDB.validateVerifier( userDN, userGroup, consumerKey, verifier )
if not result[ 'OK' ]:
  print "[ERR] %s" % result['Message']
  sys.exit( 1 )
print "ALL OK"
