from Depend import *
from Stock_Data import display_data
from Config import Configuration
import mongodb
import ast


class display_stock(Resource):

    def __init__(self):
        self.name = "DisplayStockApi"
        self.display_data = display_data()
        self.Company = Configuration().GetData()['CompanyList']

    @jwt_required
    def post(self):
        data = json.loads(request.data.decode())
        action = data["action"]

        if action == 'tickets':
            # lst = []
            # for i in mongodb.ReadAll('CompanyInfo'):
            #     temp = ast.literal_eval(i["Data"])
            #     temp["symbol"] = i["Key"].lower()
            #     lst.append(temp)
            # return lst
            lst = [
                {"Company_Name": "Apple Inc.", "Company_url": "https://logo.clearbit.com/Apple.com","symbol": "aapl"},
                {"Company_Name": "Alphabet Inc.", "Company_url": "https://logo.clearbit.com/Alphabet.com", "symbol": "goog"},
                {"Company_Name": "Amazon.com Inc.","Company_url": "https://logo.clearbit.com/Amazon.com", "symbol": "amzn"},
                {"Company_Name": "Microsoft Corporation", "Company_url": "https://logo.clearbit.com/Microsoft.com", "symbol": "msft"}
                # {"Company_Name": "General Electric Company", "Company_url": "https://logo.clearbit.com/GeneralElectricCompany.com",
                #  "symbol": "GE"}
            ]
            return lst

        elif action == 'IntraDay':
            company_name = data['ticket_name']
            interval_time = data['Interval']
            # counter = data['counter']
            return self.display_data.display_IntraDay_Data(company_name, interval_time)

        elif action == 'Daily':
            company_name = "aapl"
            return self.display_data.display_Daily_Data(company_name)

        elif action == 'Weekly':
            company_name = data['ticket_name']
            return self.display_data.display_Weekly_Data(company_name)

        elif action == 'Monthly':
            company_name = data['ticket_name']
            return self.display_data.display_Monthly_Data(company_name)





