import pandas as pd
import openpyxl as px
import psycopg2 as pg

class HealthInsuranceModel():
    def __init__(self,cp,age):
        self.cp = cp
        self.age = age
        
    # //*  Database Connectivity Settings
    def dbConnect(self):
        conn = pg.connect(
        database="healthinsurance", user='postgres', password='4658', host='localhost', port= '2450')
        return conn
    
    # //* Function to Handel UTF-8 encoded error
    def UTF8_Error_Handling(self):
        #//*  UTF-8 Error handling
        db  = self.dbConnect()
        cr = db.cursor()
        sql ='''SET client_encoding TO 'utf8';'''
        cr.execute(sql)
        db.commit()
        db.close()
    
            
    # //* Function to fetch Master Data and send it to Fetched Data Table for further Query        
    def CoverPlan(self):
        
        self.UTF8_Error_Handling()
        #//*----select count('cover_plan') from masterdata where cover_plan = '10 Lacs' and minimum <= 31 and maximum >= 31;
        
        #//* Getting Master Data from DataBase
        db  = self.dbConnect()
        cr = db.cursor()
        cr.execute('''select * from masterdata;''')
        df_list = cr.fetchall()
        db.commit()
        db.close()


        #//* Creation of new table from master data with respect to given Cover Plan and Age
        #//*Doping fetched data table if already exists.
        db  = self.dbConnect()
        cr = db.cursor()
        cr.execute("DROP TABLE IF EXISTS fetchedData")
        
        cr = db.cursor()
        sql ='''CREATE TABLE fetchedData(
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
        
        f_val = [] 
        # //* iteration through each row of master data 
        for i in df_list:         
            df_age = str(i[7])
            df_age = df_age.split('-')
        
             
        # //*flag to manage age range
            flag = False
            if len(df_age) != 1:         
                if self.age >= float(df_age[0]) and self.age <= float(df_age[1]):
                    flag = True
            else:            
                if self.age == float(df_age[0]):
                    flag = True
                        
            # //* fetching data from master data with respect to CoverPlan and Age 
            #//* Pushing the satisfied data to the Data base  
            if i[6] == self.cp and flag == True:
                f_val.append(i) 
        try:
            db  = self.dbConnect()
            cr = db.cursor()
                                
            for i in f_val:
                sql = '''INSERT INTO fetchedData values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                cr.execute(sql,i)          
            db.commit()
            db.close()
  
        except (Exception, pg.Error) as error:
                print("Failed to insert record into fetched table", error) 
                return None         
        # for i in f_val:
        #     print(i[0],i[1],i[2])
        return (f_val)
    
    # //*------To Find Insurar Name, Insurar Plan, Age Group, Cover Plan ----*//
    def fetched_static_data(self,sr_no):
        db = self.dbConnect()
        cr = db.cursor()
        cr.execute('Select Insurer_Name,Insurance_Plan,Age_Range,Cover_Plan from fetcheddata where sr_no ='+str(sr_no))
        data = cr.fetchall()
        return data[0]
    

    # //*------------Brand Existance Data----------------------*//  
    
    def Brand_Existence_Rating(self,brand_existence):
        brand_existence = int(brand_existence)
        db = self.dbConnect()
        cr = db.cursor()
        cr.execute('SELECT brand_existence,rating from rational_rating' )
        data = cr.fetchall()
        # print(data)
        flag = False
        for i in range(len(data)):
            # print(i[0])
            
            year_range = data[i][0].split('-')
            # print(year_range)
            # print(len(year_range))
            
            if len(year_range) == 1:
                # print(year_range[0][1:])
                if int(year_range[0][1:]) <= brand_existence:
                    flag = True
            elif len(year_range) == 2:
                year_range[0] = int(year_range[0])
                year_range[1] = int(year_range[1])
                # print(year_range[0],year_range[1],brand_existence)
                if brand_existence >= year_range[0] and  brand_existence <= year_range[1]:
                    flag = True
                    
                    # print(data[i][1])
        
        # print(rating)  
                    return data[i][1]
                    
            
                
                    
                

    
    
    # //*------------Final Model Data----------------------*//    
    def model_data(self):
        f_data = self.CoverPlan()
        m_list = []
        for i in f_data:    
            static_data = self.fetched_static_data(i[0])
            i_name = static_data[0]
            i_plan = static_data[1]
            age_range = static_data[2]
            cover_plan = static_data[3]
            
            brand_existance_rating = self.Brand_Existence_Rating(i[2])
            # print(i[2],brand_existance_rating)


            
            m_dict = {
                'Insurer_Name' : i_name,
                'Insurance_Plan' : i_plan,
                'Age Range' : age_range,
                'Cover Plan' : cover_plan,
                'Brand Existance Rating' : brand_existance_rating,
                
            }
            m_list.append(m_dict)
            
            
        for j in m_list:
            print(j)

   
         
obj1 = HealthInsuranceModel('50 Lacs',23)
obj1.model_data()

        
        
