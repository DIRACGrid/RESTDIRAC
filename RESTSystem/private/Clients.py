from WebAPIDIRAC.WebAPISystem.private.BottleOAManager import gOAData

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient

def __prepareArgs( kwargs ):
  if gOAData.userDN:
    kwargs[ 'delegatedGroup' ] = str( gOAData.userGroup )
    kwargs[ 'delegatedDN' ] = str( gOAData.userDN )
  kwargs[ 'useCertificates' ] = True
  return kwargs

def getRPCClient( *args, **kwargs ):
  kwargs = __prepareArgs( kwargs )
  return RPCClient( *args, **kwargs )

def getTransferClient( *args, **kwargs ):
  kwargs = __prepareArgs( kwargs )
  return TransferClient( *args, **kwargs )
