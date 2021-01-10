from pymongo import MongoClient


def main():
    client = MongoClient("localhost", 27017)
    db = client['financial_data']
    balance_sheet = db.balance_sheet_test
    all = balance_sheet.find({})
    for ticker in all:
        company = ticker['_id']
        print(company)
        entries = sorted(ticker.keys())[-3]
        print(entries)


if __name__ == '__main__':
    main()
