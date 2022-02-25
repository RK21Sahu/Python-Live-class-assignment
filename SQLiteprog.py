import os
import sqlite3
import logging
import codecs

logging.basicConfig(filename="QSLite.log", level=logging.INFO, format = '%(asctime)s %(levelname)s %(message)s')

class SQLoperation():
    """
    this call has method to create tables from inputed text file and do some operation
    """
    def sqliteoperation(self,filename1,filename2,filename3,filename4,filename5):
        """
        This method create a SQLite data base and creates 5 tables for the 5 input text file
        :param filename1:
        :param filename2:
        :param filename3:
        :param filename4:
        :param filename5:
        :return:
        """
        tab_list1 = []
        tab_list2 = []
        tab_list3 = []
        tab_list4 = []
        tab_list5 = []
        try:
            db=sqlite3.connect("vocab.db")
            c=db.cursor()
        except Exception as e:
            logging.exception("sqlite3 not connected" + str(e))
        try:
            c.execute("create table kos(col1 text)")
            with codecs.open(filename1,"r",encoding='utf-8') as f:
                #tab_list.append([tuple(x) for x in f.read().split(' ')])
                tab_list1 = [tuple(line.split()) for line in f]

            #for i in tab_list:
            #    print(i)
            c.executemany("insert into kos(col1) values(?)",tab_list1)
            db.commit()
            #data = c.execute("select * from kos")
            #for i in data:
            #    print(i)
            logging.info("data inserted successfully in kos table")
        except Exception as e:
            logging.exception("Error in kos Table" + str(e))

        try:
            c.execute("create table enron(col1 text)")
            with codecs.open(filename2,"r",encoding='utf-8') as f:
                tab_list2 = [tuple(line.split()) for line in f]
            c.executemany("insert into enron(col1) values(?)",tab_list2)
            db.commit()
            logging.info("data inserted successfully in enron table")
        except Exception as e:
            logging.exception("Error in enron Table" + str(e))

        try:
            c.execute("create table nips(col1 text)")
            with codecs.open(filename3,"r",encoding='utf-8') as f:
                tab_list3 = [tuple(line.split()) for line in f]
            c.executemany("insert into nips(col1) values(?)",tab_list3)
            db.commit()
            logging.info("data inserted successfully in nips table")
        except Exception as e:
            logging.exception("Error in nips Table" + str(e))

        try:
            c.execute("create table nytimes(col1 text)")
            with codecs.open(filename4,"r",encoding='utf-8') as f:
                tab_list4 = [tuple(line.split()) for line in f]
            c.executemany("insert into nytimes(col1) values(?)",tab_list4)
            db.commit()
            logging.info("data inserted successfully in nytimes table")
        except Exception as e:
            logging.exception("Error in nytimes Table" + str(e))

        try:
            c.execute("create table pubmed(col1 text)")
            with codecs.open(filename5,"r",encoding='utf-8') as f:
                tab_list5 = [tuple(line.split()) for line in f]
            c.executemany("insert into pubmed(col1) values(?)",tab_list5)
            db.commit()
            logging.info("data inserted successfully in pubmed table")

        except Exception as e:
            logging.exception("Error in pubmed Table" + str(e))

    def droptable(self):
        """
        This method will drop the created tables fro the database
        :return:
        """
        try:

            db=sqlite3.connect("vocab.db")
            c=db.cursor()
            c.execute("drop table kos")
            c.execute("drop table enron")
            c.execute("drop table nips")
            c.execute("drop table nytimes")
            c.execute("drop table pubmed")
            logging.info("Tables dropped successfully")
        except Exception as e:
            logging.exception("Error while dropping tables" + str(e))

    def countdup(self):
        """
        this method will do the operation of SQLite
        :return:
        """
        try:
            db=sqlite3.connect("vocab.db")
            c=db.cursor()
            #kos_q1=[]
            #kos_q2 = []
            #kos_q3 = []
            kos = []
            enron = []
            nips = []
            nytimes = []
            pubmed = []
            kos_data = c.execute("select col1 from kos")
            for i in kos_data:
                kos.append(i[0])
            logging.info("kos Tables data stored in kos List")

            enron_data = c.execute("select col1 from enron")
            for i in enron_data:
                enron.append(i[0])
            logging.info("enron Tables data stored in enron List")

            nips_data = c.execute("select col1 from nips")
            for i in nips_data:
                nips.append(i[0])
            logging.info("nips Tables data stored in nips List")

            nytimes_data = c.execute("select col1 from nytimes")
            for i in nytimes_data:
                nytimes.append(i[0])
            logging.info("nytimes Tables data stored in nytimes List")

            pubmed_data = c.execute("select col1 from pubmed")
            for i in pubmed_data:
                pubmed.append(i[0])
            logging.info("pubmed Tables data stored in pubmed List")

        except Exception as e:
            logging.exception("Error lists of data" + str(e))


        try:
            kQ1 = c.execute("select col1, count(col1) from kos group by col1")
            EQ1 = c.execute("select col1, count(col1) from enron group by col1")
            NQ1 = c.execute("select col1, count(col1) from nips group by col1")
            NYQ1 = c.execute("select col1, count(col1) from nytimes group by col1")
            PQ1 = c.execute("select col1, count(col1) from pubmed group by col1")

            logging.info("question 1 operation completed successfully and stored the data in KQ1, EQ1, NQ1, NYQ1 and PQ1 variable")
            #for i in Q1:
                #kos_q1.append(i)
                #print(i)

        except Exception as e:
            logging.exception("Error in the SQLite Q1" + str(e))

        try:
            KQ2 = c.execute("select SUBSTRING(col1,1,1), count(SUBSTRING(col1,1,1))  from kos group by SUBSTRING(col1,1,1)")
            EQ2 = c.execute("select SUBSTRING(col1,1,1), count(SUBSTRING(col1,1,1))  from enron group by SUBSTRING(col1,1,1)")
            NQ2 = c.execute("select SUBSTRING(col1,1,1), count(SUBSTRING(col1,1,1))  from nips group by SUBSTRING(col1,1,1)")
            NYQ2 = c.execute("select SUBSTRING(col1,1,1), count(SUBSTRING(col1,1,1))  from nytimes group by SUBSTRING(col1,1,1)")
            PQ2 = c.execute("select SUBSTRING(col1,1,1), count(SUBSTRING(col1,1,1))  from pubmed group by SUBSTRING(col1,1,1)")
            logging.info("question 2 operation completed successfully and stored the data in KQ2, EQ2, NQ2, NYQ2 and PQ2 variable")
            #for i in Q2:
                #kos_q2.append(i)
            #    print(i)
        except Exception as e:
            logging.exception("Error in the SQLite Q2" + str(e))

        try:
            KQ3 = c.execute("select col1  from kos where col1 NOT LIKE '%[^a-z]%'")
            EQ3 = c.execute("select col1  from enron where col1 NOT LIKE '%[^a-z]%'")
            NQ3 = c.execute("select col1  from nips where col1 NOT LIKE '%[^a-z]%'")
            NYQ3 = c.execute("select col1  from nytimes where col1 NOT LIKE '%[^a-z]%'")
            PQ3 = c.execute("select col1  from pubmed where col1 NOT LIKE '%[^a-z]%'")
            logging.info("question 3 operation completed successfully and stored the data in KQ3, EQ3, NQ3, NYQ3 and PQ3 variable")

            #for i in KQ3:
            #    kos_q3.append(i)
            #    print(i)
        except Exception as e:
            logging.exception("Error in the SQLite Q3" + str(e))

        try:
            Q4 = list(zip(kos, enron, nips,nytimes,pubmed))
            for i in Q4:
                print(i)
            logging.info("question 4 operation completed successfully and stored in Q4 variable")

        except Exception as e:
            logging.exception("Error in the SQLite Q4" + str(e))



