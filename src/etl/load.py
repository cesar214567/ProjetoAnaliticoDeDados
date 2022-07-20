import psycopg2
import pandas as pd
import csv

class Loader:
    
    def __loadTable__(connection,cursor, fileName, tableName,sep=';'):
        with open(fileName, 'r') as file:
            cursor.copy_from(file, tableName, sep,null='')
        connection.commit()
        print("Table " + tableName + " inserted successfully")


    def __printSep__(text):
        print("-------------------------------------------------------------------")
        print("-------------------------------------------------------------------")
        print("                           " + text)
        print("-------------------------------------------------------------------")
        print("-------------------------------------------------------------------")


    def loadTables(self):
        try:
            connection = psycopg2.connect(user="postgres",
                                        password="admin",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="pad_constellation")
            
            cursor = connection.cursor()
            tabelas = ['ddata','dproduto','duf','dpais','fimportacoes','fprecos']
            for tabela in tabelas:
                self.__printSep__(tabela)
                self.__loadTable__(connection, cursor,tabela + '.csv', tabela,',')


            self.__printSep__('FINISHED')

        except (Exception, psycopg2.Error) as error:
            if connection:
                print("Failed to insert record", error)

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")