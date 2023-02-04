import csv
import json


# def lambda_handler(event, context):
#     # targetbucket = 'BUCKET NAME'
#     # csvkey = 'FILENAME.csv'
#     # jsonkey = 'FILENAME.json'

with open (input("Name the csv file", r) as f:
    reader = csv.reader(f)
    next(reader)
    data = {"colors": []}
    for row in reader:
        data["colors"].append({"Color":
        row[0], "Value": row[1]})

with open ("colors.json", "w") as f:
    json.dump(data, f, indent=4)

    # if __name__ == '__main__':
    #     lambda_handler()
