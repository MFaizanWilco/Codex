from Depend import *
from LSTM import lstm
from Config import Configuration


class prediction(Resource):
    def __init__(self):
        self.name = "predictionApi"
        self.lstm = lstm()
        self.Company = Configuration().GetData()['CompanyList']

    @jwt_required
    def post(self):
        data = json.loads(request.data.decode())
        user_id = get_jwt_identity()
        action = data["action"]
        company_name = data['ticket_name']
        collection = 'IntraDay'

        if action == 'lstm':
            # start_date = datetime.date.today() - timedelta(days=1)
            # end_date = datetime.date.today()
            return self.lstm.predict(company_name)

        elif action == 'selected_prediction':
            start_date = data['start_date']
            end_date = data['end_date']
            nopredictions = data['minute']
            # print(data)
            return self.lstm.selected_predict(company_name, start_date, end_date, int(nopredictions))



