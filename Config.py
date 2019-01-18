class Configuration:

    def GetData(self):

        Datadictionary = {
                # "PrivateIp": "127.0.0.1",
                "MongoPort": 27017,
                "MongoDB": "CodexDb",
                "PrivateIp": "34.211.50.116",
                "EmailID": "testcodexnow@gmail.com",
                "Password": "test_codexnow",
                "RegisterEmail": "http://34.211.50.116:27017/#/verify/email/",
                # "RegisterEmail": "http://192.168.100.116:3002/#/verify/email/",
                "ForgotPassword": "http://34.211.50.116:27017/#/forgetpassword/resetpassword/",
                # "ForgotPassword": "http://192.168.100.116:3002/#/forgetpassword/resetpassword/",
                # "team_emails": ['tariq@codexnow.com', 'anwar@codexnow.com', 'arsalan@codexnow.com',
                #                 'abdullah@codexnow.com', 'kawish@codexnow.com', 'waqar@codexnow.com', 'mubeen@codexnow.com'],
                "team_emails": ['mfaizan@codexnow.com'],
                # "Email": ['tariq@codexnow.com', 'anwar@codexnow.com','kawish@codexnow.com','adeel.farooq@codexnow.com','sherjil@codexnow.com'],
                "Email": ['mfaizan@codexnow.com'],
                "SP": 'https://www.codexnow.com',
                # "SP": 'http://192.168.100.102:3002',
                "UploadPath": "/home/codexnow/Desktop/upload",
                "generated_data_path": "/home/codexnow/GD3/",
                "StockDataMile": ["IntraDay", "Weekly", "Monthly"],
                "CompanyList": ["aapl", "amzn", "goog","msft","abbv", "abc", "abt", "act", "adbe", "adi", "adm", "adp", "adsk", "adt",
                            "aee", "aep", "aes", "aet", "afl", "agn", "aig", "aiv", "aiz", "akam", "all",
                            "altr", "alxn", "amat", "amd", "amgn", "amp", "amt", "amzn", "an", "anf", "aon",
                            "apa", "apc", "apd", "aph", "ati", "avb", "avp", "avy", "axp", "azo", "ba", "bac",
                            "bax", "bbby", "bbt", "bby", "bdx", "ben", "bf.b", "biib", "bk", "blk", "bll", "bms",
                            "bmy", "brkb", "bsx", "btu", "bwa", "bxp", "c", "ca", "cag", "cah", "cat", "cb", "cbg",
                            "xom", "xray", "xrx", "xyl", "yum", "zion", "zmh"],
                "CompanyListP": ["aapl", "amzn", "goog","msft"],
                "APIKEYDICT": ["983ACTP3EL5ZNG06", "PLAO4INA6N20GEOB", "8O6VXZEFAD4RB3GM", "PLAO4INA6N20GEOB",
                               "PLAO4INA6N20GEOB", "6N29U4IWMPL7D8T8", "B44WTJRIWDFGTZ4W"]

        }
        return Datadictionary

