=============================
OAuth Authorization
=============================

OAuth is used for authorization when using the API. This document will not discuss how OAuth works. 
To discover how to use OAuth please go `here <http://oauth.net>`_ . DIRAC Web API requires OAuth 1.0a.

Before using the API remember to get your consumer key and secret to use the API.

Using http://diracgrid.org as an example, the endpoints for the OAuth flow would be:

1. To acquire a request token http://diracgrid.org/oauth/request_token
2. Redirect the user to http://diracgrid.org/oauth/authorize to get your tokens authorized
3. Exchange the request token for an access token at http://diracgrid.org/oauth/access_token

Change http://diracgrid.org by your API server accordingly.