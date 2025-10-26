from sharepoint_api.sharepoint_client import SharePointClient

sp= SharePointClient(site='retail')
sales = sp.read_csv("sales/liverpool_473/maidenform_1450/sales_1701_1712.csv")
x=1