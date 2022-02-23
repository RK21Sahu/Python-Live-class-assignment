import pymongo
import json
import pandas as pd
import logging

logging.basicConfig(filename="mangodb.log", level=logging.INFO, format = '%(asctime)s %(levelname)s %(message)s')

class mangodata():
    def __init__(self):
        try:
            self.client =pymongo.MongoClient("mongodb+srv://mongodb:mongodb@cluster0.ocurj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",tls=True, tlsAllowInvalidCertificates=True)
            logging.info("sucessfully connected to MangoDB")
        except Exception as e:
            logging.exception("MangoDB not connected" + str(e))

    def insertData(self):
        """This method will create a data base and collection and insert the CSV data into
        mangodb collection

        """
        try:

            db = self.client.test
            db = self.client["carbon"]
            logging.info("MangoDB object {db} created for the data base")
            coll=db["nanotubes"]
            logging.info("nanotubes collection created")
            #for i in coll.find():
            #    print(i)

            df = pd.read_csv("carbon_nanotubes.csv")
            data= df.to_dict(orient="records")
            coll.insert_many(data)
            logging.info("CSV data inserted into nanotubes collection")
            #for i in data:
            #    pritn(i)
        except Exception as e:
            logging.exception("Exception occured" + str(e))

    def Dataoperation(self):

        try:

            db = self.client["carbon"]
            coll=db["nanotubes"]


            Chiral_data1 = coll.find({"Chiral_indice_n_m":"2;1;0"})
            Chiral_data2 = coll.find({"Chiral_indice_n_m": {"$in":["2;1;0","3;1;0"]}})
            Chiral_data3 = coll.find({"Calculated_atomic_coordinates_w": {"$gt": 302196}})
            Chiral_data3 = coll.find({"Calculated_atomic_coordinates_w": {"$gt": 302196}})
            logging.info("Filter and find operation using Find method executed")

            coll.update_many({"Calculated_atomic_coordinates_w": {"$gt": 302196}},{"$set":{"Calculated_atomic_coordinates_w":101}})
            logging.info("update operation using Find method executed")
            coll.delete_many({"Chiral_indice_n_m": "12;6;0"})

            logging.info("delete operation using Find method executed")

            #for i in Chiral_data3:
            #    print(i)
        except Exception as e:
            logging.exception("Exception occured" + str(e))





