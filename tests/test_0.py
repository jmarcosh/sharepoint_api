from sharepoint_api.sharepoint_client import SharePointClient

sp = SharePointClient(site='servoreso', dry_run=False,  config_path="../credentials/secrets.toml")
imports = sp.read_excel("Imports/Templates/customs.xlsx")
sp.save_excel(imports, "Imports/Templates/customs_trial.xlsx")
sp= SharePointClient(site='retail', config_path="../credentials/secrets.toml")
files = sp.list_files_in_folder("sales/liverpool_473/maidenform_1450")
sales = sp.read_csv("sales/liverpool_473/maidenform_1450/sales_1701_1712.csv")
x=1