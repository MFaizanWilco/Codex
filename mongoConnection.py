from pymongo import *

class MongoDB():
    def __init__(self):
        self.name = 'Mongo'
        self.client = None
        self.db = None

    def ConnectMongo(self, Host, Port, dbName):
        self.client = MongoClient(Host,Port)
        self.db = self.client[str(dbName)]


    def WriteValue(self,collectionName,KN,Value):
        try:
            self.collection = self.db[collectionName]
            post_data = {
                'Key': str(KN),
                'Data': str(Value)

            }
            self.collection.insert_one(post_data)

        except Exception as e:
            print(e.message)


    def UpdateValue(self,collectionName,KN,Value):
        try:
            self.collection = self.db[collectionName]
            self.collection.update_one(
                {"Key": str(KN)},
                {
                    "$set": {
                        "Data": str(Value)
                    }
                }
            )

        except Exception as e:
            print(e.message)


    def ReadValue(self,collectionName,KN):
        try:
            self.collection = self.db[collectionName]
            value = self.collection.find_one({'Key': str(KN)})
            return value

        except Exception as e:
            print(e.message)


    def ReadAll(self,collectionName):
        try:
            self.collection = self.db[collectionName]
            value = self.collection.find({})
            return value

        except Exception as e:
            print(e.message)


    def RemoveRecord(self,collectionName,userid):
        try:
            self.collection = self.db[collectionName]
            self.collection.remove({'Key': str(userid)})

        except Exception as e:
            print(e.message)
