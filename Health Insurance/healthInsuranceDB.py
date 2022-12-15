import pandas as pd
import openpyxl as px
import psycopg2 as pg
import sys

class HealthInsuranceDB():
    def __init__(self):
       pass
        
    # //*  Database Connectivity Settings
    def dbConnect(self):
        conn = pg.connect(
        database="healthinsurance", user='postgres', password='4658', host='localhost', port= '2450')
        return conn
    
    # //* Function to Handel Error from database : character with byte sequence 0xe2 0x82 0xb9 in encoding "UTF8" has no equivalent in encoding "WIN1252"
    def UTF8_Error_Handling(self):
        #//*  UTF-8 Error handling
        db  = self.dbConnect()
        cr = db.cursor()
        sql ='''SET client_encoding TO 'utf8';'''
        cr.execute(sql)
        db.commit()
        db.close()
        
     # //* Function to send Dataframe Data to Database Server   
    def Excel_To_Db(self,tbname,df_list):
        try:
            db  = self.dbConnect()
            cr = db.cursor()
            
            df_list_size = len(df_list)
            sql_val = ""
            
            #//* Creating a full string containing values for each df_list
            for i in range(0, df_list_size):
                if i != df_list_size-1:
                    sql_val = sql_val + str(df_list[i]) + ','
                    
                else:
                    # //* Else to handle comma',' of the last value
                    sql_val = sql_val + str(df_list[i])
                    
                
            cr.execute(f'INSERT INTO {tbname} values'+sql_val)
            db.commit()
            db.close()
                
        except (Exception, pg.Error) as error:
                print(f"Failed to insert record into {tbname}", error)          
        finally:
            print(f'{tbname} table function ended')
            
        return df_list
        
    
    #//* Function to add data to Master Data     
    def addMaster(self,filename,sheet):
        df = pd.read_excel(filename,sheet)
        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the Master Data  Data Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        # #//*Doping MasterData table if already exists.
        cr.execute("DROP TABLE IF EXISTS MasterData")
        cr = db.cursor()
        
        sql ='''CREATE TABLE MasterData(  
            SR_No integer,
            Insurer_Name Varchar,
            Brand_Existence integer,
            Insurance_Plan varchar,
            Unique_Code_Plan Varchar,
            Product_Existence integer,
            Cover_Plan Varchar,
            Age_Range Varchar,
            Pricing integer,
            Room_Rent Varchar,
            NCB Varchar,
            Recharge_of_SI Varchar,
            Pre_Existing_Disease Varchar,
            Co_pay Varchar,
            Health_and_Wellness Varchar,
            "Claims_Settlement_Ratio (%)" integer,
            "Incurred_Claim_Ratio_(%)" integer,
            "Ageing_of_Claim_(%)" integer,
            Network_Hospitals Varchar 
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to master Data
        # tbname = 'MasterData'
        # self.Excel_To_Db(tbname,df_list)
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into MasterData values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
            
        return df_list
    
    #//* Function to add data to Room-Rent Data     
    def addRoomRent(self,filename,sheet):
        df = pd.read_excel(filename,sheet)
        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        print(cl_name)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the Room-Rent Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        #//*Doping Room-Rent table if already exists.
        cr.execute("DROP TABLE IF EXISTS room_rent_rating")
        cr = db.cursor()
        sql ='''CREATE TABLE room_rent_rating(  
           SR_No integer ,
           Must_Look_Features Varchar,
           Sum_Insured Varchar,
           Keywords varchar,
           Ratings integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        #//*  adding values to Room Rent Data
        # tbname = 'room_rent_rating'
        # self.Excel_To_Db(tbname,df_list)
        
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into room_rent_rating values(%s,%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
    #//* Function to add data to Pre-Existing-Disease Data     
    def addPreExistingDisease(self,filename,sheet):
        df = pd.read_excel(filename,sheet)
        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
       #//* SQL Query to insert the Pre-Existing-Disease Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        #//*Doping Pre-Existing-Disease table if already exists.
        cr.execute("DROP TABLE IF EXISTS pre_existing_disease")
        cr = db.cursor()
        
        sql ='''CREATE TABLE pre_existing_disease(  
          SR_No integer,
          Must_Look_Features Varchar,
          Keywords Varchar,
          Ratings integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to Pre Existing Disease Data
        # tbname = 'pre_existing_disease'
        # self.Excel_To_Db(tbname,df_list)
        
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
          
            sql = '''Insert into pre_existing_disease values(%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
    #//* Function to add data to Recharge Of SI Data     
    def addRechargeOfSI(self,filename,sheet):
        df = pd.read_excel(filename,sheet)
        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)

        self.UTF8_Error_Handling()
        
        # #//* SQL Query to insert the Recharge Of SI Data  Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        #//*Doping Recharge Of SI table if already exists.
        cr.execute("DROP TABLE IF EXISTS recharge_of_si")
        cr = db.cursor()
        sql ='''CREATE TABLE recharge_of_si(  
         SR_No integer,
         Must_Look_Features varchar,
         Keywords varchar,
         Ratings integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to Recharge Of SI Data
        # tbname = 'recharge_of_si'
        # self.Excel_To_Db(tbname,df_list)
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into recharge_of_si values(%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
    
    #//* Function to add data to Rational Rating Data     
    def addRationalRating(self,filename,sheet):
        df = pd.read_excel(filename,sheet)
        # print(df)
        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the Rational Rating Data  Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        #//*Doping Rating Data table if already exists.
        cr.execute("DROP TABLE IF EXISTS rational_rating")
        cr = db.cursor()
        sql ='''CREATE TABLE rational_rating(  
        Pricing Varchar,
        Room_Rent Varchar,
        NCB Varchar,
        Recharge_of_SI Varchar,
        Pre_Existing_Disease Varchar,
        Co_pay Varchar,
        Health_and_Wellness Varchar,
        Claims_Settled Varchar,
        Incurred_Claim_Ratio Varchar,
        "Ageing_of_Claim (%)" Varchar,
        Network_Hospitals Varchar,
        Brand_Existence Varchar,
        Product_Existence Varchar,
        Rating integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to Rational Rating Data
        # tbname = 'rational_rating'
        # self.Excel_To_Db(tbname,df_list)
        
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into rational_rating values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
      
      
         
    #//* Function to add data to CO Pay Data         
    def addCoPay(self,filename,sheet):
        df = pd.read_excel(filename,sheet)

        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the CO Pay Data  Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        #//*Doping CO Pay table if already exists.
        cr.execute("DROP TABLE IF EXISTS co_pay")
        
        cr = db.cursor()
        sql ='''CREATE TABLE co_pay(  
        SR_No integer,
        Must_Look_Features Varchar,
        Keywords Varchar,
        Ratings Varchar
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to CO Pay Data
        # tbname = 'co_pay'
        # self.Excel_To_Db(tbname,df_list)
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into co_pay values(%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
      
        
        
    #//* Function to add data to Weightage Data         
    def addWeightage(self,filename,sheet):
        df = pd.read_excel(filename,sheet)

        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        # #//* SQL Query to insert the Weightage Data  Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        #//*Doping Weightage table if already exists.
        cr.execute("DROP TABLE IF EXISTS weightage")
        
        cr = db.cursor()
        sql ='''CREATE TABLE weightage(  
        Sno integer,
        Parameters Varchar,
        "Weightage (%)" Integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # #//*  adding values to Weightage Data
        # tbname = 'weightage'
        # self.Excel_To_Db(tbname,df_list)
        
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into  weightage values(%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
         
    #//* Function to add data to Pros and Cons         
    def addProsCons(self,filename,sheet):
        df = pd.read_excel(filename,sheet)

        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the Pros and Cons   Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        # #//*Doping Pros and Cons  table if already exists.
        cr.execute("DROP TABLE IF EXISTS pros_cons")
        
        cr = db.cursor()
        
        sql ='''CREATE TABLE pros_cons(  
       Pricing Varchar,
       Room_Rent Varchar,
       NCB Varchar,
       Recharge_of_SI Varchar,
       Pre_Existing_Disease Varchar,
       Co_pay Varchar,
       Health_and_Wellness Varchar,
       Claims_Settled Varchar,
       Incurred_Claim_Ratio Varchar,
       Ageing_of_Claim Varchar,
       Network_Hospitals Varchar,
       Brand_Existence Varchar,
       Product_Existence Varchar,
       Rating integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # # #//*  adding values to Pros and Cons  Data
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into  pros_cons values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
          
    #//* Function to add data to NCB         
    def addNCB(self,filename,sheet):
        df = pd.read_excel(filename,sheet)

        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        #//* SQL Query to insert the NCB  Data Frame to DataBase 
        db  = self.dbConnect()
        cr = db.cursor()
        
        # #//*Doping NCB table if already exists.
        cr.execute("DROP TABLE IF EXISTS ncb")
        
        cr = db.cursor()
        sql ='''CREATE TABLE ncb(  
       SR_No Integer,
       Must_Look_Features Varchar,
       Keywords Varchar,
       Ratings Integer
        )'''
        cr.execute(sql)
        print("Table created successfully........")
        db.commit()
        db.close()
        
        # # #//*  adding values to NCB Data
        db  = self.dbConnect()
        cr = db.cursor()
        for i in df_list:
            sql = '''Insert into ncb values(%s,%s,%s,%s)'''
            cr.execute(sql,i)
        db.commit()
        db.close()
        
        
     #//* Function to add data to Health and Wellness        
    def addHealthAndWellness(self,filename,sheet):
        df = pd.read_excel(filename,sheet)

        df.fillna('Not_Available', inplace = True)
        df_list = list(df.itertuples(index=False, name=None))

        cl_name = list(df.columns)
        self.UTF8_Error_Handling()
        
        
        try : 
              #//* SQL Query to insert the Health and Wellness  Data Frame to DataBase 
            db  = self.dbConnect()
            cr = db.cursor()
            
            # #//*Doping NCB table if already exists.
            cr.execute("DROP TABLE IF EXISTS health_wellness")
            
            cr = db.cursor()
            sql ='''CREATE TABLE health_wellness(  
            SR_No Integer,
            Must_Look_Features Varchar,
            Unique_Code_Plan Varchar,
            Ratings Integer
                )'''
            cr.execute(sql)
            print("Table created successfully........")
            db.commit()
            db.close()
            
            #//*  adding values to Health and Wellness Data
            db  = self.dbConnect()
            cr = db.cursor()
            for i in df_list:
                sql = '''Insert into health_wellness values(%s,%s,%s,%s)'''
                cr.execute(sql,i)
            db.commit()
            db.close()
            
        except:
            exc_tuple = sys.exc_info()
            for i in exc_tuple:
                print(i)
            
            
   
         
obj1 = HealthInsuranceDB()
path = 'D:\Atrina\Health Insurance\Excel Sheet\HI_DATA(13-12-2022).xlsx'
# obj1.addMaster(path,'MasterData')
# obj1.addRoomRent(path,'Room Rent')
# obj1.addPreExistingDisease(path,'Pre-Existing Disease')
# obj1.addRechargeOfSI(path,'Recharge of SI')
# obj1.addRationalRating(path,'SQL_R')
# obj1.addCoPay(path,'Co-Pay')
# obj1.addWeightage(path,'Weightage')
# obj1.addProsCons(path,'Pros and Cons')
# obj1.addNCB(path,'NCB')
obj1.addHealthAndWellness(path,'Health and Wellness')
        
        
