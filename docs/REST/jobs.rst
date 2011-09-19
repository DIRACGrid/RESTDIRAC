.. _GET /jobs:

=================
GET /jobs
=================

Returns a list of jobs that match the requirements. For users this list will typically be a list of his own jobs.

* Parameters

.. list-table:: 
  :header-rows: 1
  :widths: 20 80
  
  * - Parameter
    - Description
  * - status
    - Comma separated list of requested job status
  * - minor_status
    - Comma separated list of request job minor status 

.. _POST /jobs:

=================
POST /jobs
=================

Submits a new job to DIRAC. Job has to be encoded in a JSON associative array

* Parameters