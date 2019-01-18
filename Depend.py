# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request, json,send_file,redirect,session,render_template
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
from datetime import datetime
import datetime
import time
import json
import os


from flask_jwt_extended import (JWTManager,jwt_required, create_access_token,
    jwt_refresh_token_required, create_refresh_token,get_raw_jwt,
    get_jwt_identity)

from JWTConfig import JWT

from usersApi import userApi
from prediction_Api import prediction
from DisplayStocks_Api import display_stock
from Config import Configuration
import flask_monitoringdashboard as dashboard