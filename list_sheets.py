import gspread
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE = 'omega-vx-service-account.json'
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
gc = gspread.authorize(creds)

# List all spreadsheets the service account can access
for f in gc.list_spreadsheet_files():
    print(f['name'])
