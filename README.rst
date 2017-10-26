You need to:

* add `REST` to the list of Extensions `/DIRAC/Extensions`
* Create a new section `REST` in which you add `CodeAuthURL=Something`
* In `/DIRAC/Setups/<yoursetup>`, add a config for `REST`
* In the `Systems` section, add a `REST` section in which you put what is in the `RESTDIRAC/RESTSystem/ConfigTemplate.cfg` file, and define the OATokenDB

Even if you do not want to use OAuth, it is all mandatory.

Start the server with `python RESTDIRAC/RESTSystem/scripts/dirac-rest-server.py`.
By default it listens to port 9910
