
import json
from pymongo import MongoClient
from datetime import datetime
from collections import defaultdict

def collection(uri):
    client = MongoClient(uri)
    database = client["rhobs"]
    collection = database["people"]
    return collection


def load(uri="localhost", datapath="data.json"):
    coll = collection(uri=uri)
    with open('C:/Users/willy/PycharmProjects/RHOBS_Project/data.json.codechallenge.janv22.RHOBS.json', 'r') as fp:
        data = json.load(fp)

        result = coll.insert_many(data)
        print(len(result.inserted_ids), "documents ont été insérés dans la collection.")


#Tri des travailleurs selon le sexe
def count_by_gender(uri="localhost"):
    coll = collection(uri=uri)
    count_women = coll.count_documents({"sex": "F"})
    count_men = coll.count_documents({"sex": "M"})
    return count_women, count_men

#Decompte des entreprises suivant le nombre de personnes
def get_companies_with_n_employees(N, uri="mongodb://localhost:27017/"):
    coll = collection(uri=uri)
    companies = coll.aggregate([
        {"$group": {"_id": "$company", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": N}}}
    ])
    return list(companies)


#Pyramide des ages
def age(birthdate):
    return int((datetime.now() - datetime.strptime(birthdate, "%Y-%m-%d")).days / 365.25)

def pyramid_by_job(uri="localhost"):
    coll = collection(uri=uri)
    jobs = input("Entrez un ou plusieurs métiers (séparés par des virgules) : ")
    job_list = [job.strip() for job in jobs.split(",")]
    query = {"job": {"$in": job_list}}
    cursor = coll.find(query)
    ages = defaultdict(int)
    for person in cursor:
        ages[age(person["birthdate"])] += 1
    return ages


if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    datapath = "data.json"
    load(uri, datapath)
    count_women, count_men = count_by_gender(uri=uri)
    print("Nombre de femmes :", count_women)
    print("Nombre d'hommes :", count_men)
    N = 10
    companies = get_companies_with_n_employees(N, uri=uri)
    print(f"Entreprises avec plus de {N} employés :")
    for company in companies:
        print(f"{company['_id']} ({company['count']} employés)")
        print()
#
    ages = pyramid_by_job(uri=uri)
    print("Pyramide des âges pour les métiers :", ", ".join(str(age) + " ans" for age in ages.keys()))
    for age, count in sorted(ages.items()):
        print(f"{age}-{age + 9}: {count}")