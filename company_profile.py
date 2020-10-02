import requests
import json

url =  'https://datafied.api.edgar-online.com/v2/companies?filter=primarysymbol%20eq%20%22AAPL%22&appkey=de010557d283d47912805d7666fed46e'


requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
try:
    requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
except AttributeError:
    # no pyopenssl support used / needed / available
    pass

page = requests.get(url, verify=False)
result = page.json()
items = result['result']['rows'][0]['values']
company_profile = {}
for item in items:
    company_profile[item['field']] = item['value']

print(company_profile)
