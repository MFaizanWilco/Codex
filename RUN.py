from Depend import *

application = Flask(__name__)
application.config['JWT_SECRET_KEY'] = 'codex-team'
application.config['JWT_ACCESS_TOKEN_EXPIRES'] = datetime.timedelta(hours=24)
application.config['JWT_BLACKLIST_ENABLED'] = True
application.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
jwt = JWTManager(application)
CORS(application)
api = Api(application)
jwt_auth = JWT()
blacklist = set()

@application.route('/login', methods=['POST'])
def auth():
    return jwt_auth.login(request)

@application.route('/refresh', methods=['POST'])
def refresh():
    return jwt_auth.refresh()

@application.route('/get_user', methods=['GET'])
def protected():
    return jwt_auth.get_user()

@application.route('/get_history', methods=['GET'])
def get_hist():
    return jwt_auth.get_history()

@jwt.token_in_blacklist_loader
def check_if_token_in_blacklist(decrypted_token):
    jti = decrypted_token['jti']
    return jti in blacklist

@application.route('/logout', methods=['DELETE'])
@jwt_required
def logout():
    jti = get_raw_jwt()['jti']
    blacklist.add(jti)
    return jwt_auth.logout()

@application.route('/logout2', methods=['DELETE'])
@jwt_refresh_token_required
def logout2():
    jti = get_raw_jwt()['jti']
    blacklist.add(jti)
    return jwt_auth.logout()

api.add_resource(userApi, '/user')
# api.add_resource(ibConnApi, '/con')
api.add_resource(prediction, '/prediction')
api.add_resource(display_stock, '/display')
# api.add_resource(Controller, '/Jupyter')

dashboard.config.init_from('config.cfg')
dashboard.bind(application)

if __name__ == '__main__':
    # context = ('codexnow.crt', 'codexnow.key')
    application.run(debug=True, host='0.0.0.0', port=5005, threaded=True)
