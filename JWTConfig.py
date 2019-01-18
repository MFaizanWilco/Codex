from flask_jwt_extended import (
    JWTManager, jwt_required, get_jwt_identity,
    create_access_token, create_refresh_token,
    jwt_refresh_token_required, get_raw_jwt
)
from flask import jsonify,session
from mongoConnection import MongoDB
from Config import Configuration
import ast
import JsonEncoder as json
import requests
import json
from logger import generate_log

class JWT:
    def login(self, request):
        try:
            data = json.loads(request.data.decode())
            self.ip = Configuration().GetData()['PrivateIp']
            self.port = Configuration().GetData()['MongoPort']
            self.db = Configuration().GetData()['MongoDB']
            obj = MongoDB()
            obj.ConnectMongo(self.ip, self.port, self.db)
            record = obj.ReadValue("users", data["email"])
            if (record != None):
                record = ast.literal_eval(record['Data'])
                if (record['password'] == data["password"]):
                    ret = {
                        'access_token': create_access_token(identity=data["email"]),
                        'refresh_token': create_refresh_token(identity=data["email"]),
                        'status': "True"

                    }
                    return jsonify(ret), 200
                else:
                    return jsonify({"status": "Invalid username or password"}), 401
            else:
                return jsonify({"status": "Invalid username or password"}), 401
        except Exception as e:
            generate_log('auth', str(e), str(request))


    def logout(self):
        return jsonify({"status": "Successfully logged out"}), 200

    @jwt_refresh_token_required
    def logout2(self):
        return ({"status": "Successfully logged out"}), 200

    @jwt_refresh_token_required
    def refresh(self):
        try:
            current_user = get_jwt_identity()
            ret = {
                'access_token': create_access_token(identity=current_user),
                'refresh_token': create_refresh_token(identity=current_user),
                'status': "Successfully Refreshed"
            }
            return jsonify(ret), 200
        except Exception as e:
            generate_log('refresh', str(e), 'Creating Refresh Token')

    @jwt_required
    def get_user(self):
        try:
            email = get_jwt_identity()
            self.ip = Configuration().GetData()['PrivateIp']
            self.port = Configuration().GetData()['MongoPort']
            self.db = Configuration().GetData()['MongoDB']
            obj = MongoDB()
            obj.ConnectMongo(self.ip, self.port, self.db)
            record = obj.ReadValue("users", email)
            record = ast.literal_eval(record['Data'])
            record.pop('password', None)
            return jsonify(record)
        except Exception as e:
            generate_log('get_user', str(e), 'get_user method')

    @jwt_required
    def get_history(self):
        try:
            email = get_jwt_identity()
            self.ip = Configuration().GetData()['PrivateIp']
            self.port = Configuration().GetData()['MongoPort']
            self.db = Configuration().GetData()['MongoDB']
            obj = MongoDB()
            obj.ConnectMongo(self.ip, self.port, self.db)
            record = obj.ReadValue("history", email)
            if record != None:
                record = ast.literal_eval(record["Data"])
                toreturn = {"status":"True","record":record}

            else:
                # record = ast.literal_eval(record["Data"])
                toreturn = {"status": "False"}
            return jsonify(toreturn)
        except Exception as e:
            generate_log('get_history', str(e))
