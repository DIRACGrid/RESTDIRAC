

class OAuthDataStore():

  def getConsumerSecret( self, consumerKey ):
    return "%sSECRET" % consumerKey


  def generateTokenPair( self, consumerKey, request = False ):
    token = "%s%sTOKEN" % ( consumerKey, request )
    return ( token, "%sSECRET" % token )

  def getTokenSecret( self, consumerKey, requestToken, request = False ):
    return "%sSECRET" % requestToken


  def getRequestVerifier( self, consumerKey, requestToken ):
    return "%s%sVerifier" % ( consumerKey, requestToken )

  def generateRequestVerifier( self, consumerKey, requestToken ):
    return "%s%sVerifier" % ( consumerKey, requestToken )
