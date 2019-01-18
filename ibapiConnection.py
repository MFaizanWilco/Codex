from Depend import *
from IbConnection import *
import IbConnection

class IbApiConnection:
    def __init__(self):
        self.name="ibapiconnection"
        self.IbConnection = IbConnection()

    def post(self):
        data = json.loads(request.data.decode())
        action = data["action"]

        if action == 'GetDataFromIBAPI':
            return self.IbConnection.getData()
        elif action == 'SetDataToIBAPI':
            return self.IbConnection.SetData()
