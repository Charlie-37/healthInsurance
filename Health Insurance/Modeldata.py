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
        cr.execute('''select * from masterdata ORDER BY sr_no;''')
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
        # print(f_val)
        return (f_val)
    
    # //*------To Find Insurar Name, Insurar Plan, Age Group, Cover Plan ----*//
    def fetched_static_data(self,sr_no):
        db = self.dbConnect()
        cr = db.cursor()
        cr.execute('Select Insurer_Name,Insurance_Plan,Age_Range,Cover_Plan, sr_no from fetcheddata where sr_no ='+str(sr_no)+';')
        data = cr.fetchall()
        db.commit()
        db.close()
        return data[0]
    

    # //*------------Brand Existance Data----------------------*//  
    
    def Brand_Existence_Rating(self,brand_existence):
        brand_existence = int(brand_existence)
        db = self.dbConnect()
        cr = db.cursor()
        cr.execute('SELECT brand_existence,rating from rational_rating;' )
        data = cr.fetchall()
        db.commit()
        db.close()
        #//* flag is used that the check if range is satisfied by the given year
        flag = False
        rating = 0
        for i in range(len(data)): 
            
            # //*fetched out the two years from brand exis. range   
            year_range = data[i][0].split('-')
            
            # //*if age range is of single year
            if len(year_range) == 1:
                if int(year_range[0][1:]) <= brand_existence:
                    flag = True
                    rating = data[i][1]
            elif len(year_range) == 2:                
                if brand_existence >= int(year_range[0]) and  brand_existence <= int(year_range[1]):
                    flag = True
                    # return data[i][1]
                    rating = data[i][1]
          
        if flag == True:        
            return rating
        else:
            return 'Not_Available'
                
    # //*------------Product Existance Data----------------------*//  
    
    def Product_Existence_Rating(self,product_existence):
        product_existence = int(product_existence)
        db = self.dbConnect()
        cr = db.cursor()
        cr.execute('''SELECT product_existence,rating from rational_rating; '''  )
        data = cr.fetchall()

        db.commit()
        db.close()
        
        #//*--Same logic as above brand existance function
        flag = False
        rating = 0
        for i in range(len(data)):    
            year_range = data[i][0].split('-')
            
            if len(year_range) == 1:
                if int(year_range[0][1:]) <= product_existence:
                    flag = True
                    rating = data[i][1]
            elif len(year_range) == 2:                
                if product_existence >= int(year_range[0]) and  product_existence <= int(year_range[1]):
                    flag = True
                    # return data[i][1]
                    rating = data[i][1]
          
        if flag == True:        
            return rating
        else:
            return 'Not_Available'                    
                

    # //*------------Room Rent Data----------------------*//   
    
    def RoomRent(self,cover_plan,rrData):
        db = self.dbConnect()
        cr = db.cursor()  
        tpl = (cover_plan,rrData)
        sql = ''' select ratings from room_rent_rating where sum_insured =%s and keywords = %s;'''
        cr.execute(sql,tpl)
        
        data = cr.fetchone()        
        db.commit()
        db.close()
        
        return data
        

    
    # //*------------Final Model Data----------------------*//    
    def model_data(self):
        f_data = self.CoverPlan()
        m_list = []
        for i in f_data:    
            static_data = self.fetched_static_data(i[0])
            sr_no = static_data[4]
            i_name = static_data[0]
            i_plan = static_data[1]
            age_range = static_data[2]
            cover_plan = static_data[3]
            
            brand_existance_rating = self.Brand_Existence_Rating(i[2])
            product_existance_rating = self.Product_Existence_Rating(i[5])
            room_rent = self.RoomRent(i[6],i[9])
            
            if room_rent == None:
                room_rent = 'Not_Available'
            else:
                room_rent = room_rent[0]



            
            m_dict = {
                'Sr No' : sr_no,
                'Insurer_Name' : i_name,
                'Insurance_Plan' : i_plan,
                'Age Range' : age_range,
                'Cover Plan' : cover_plan,
                'Brand Existance Rating' : brand_existance_rating,
                'Product_Existence_Rating' : product_existance_rating,
                'room_rent_rating' : room_rent,
                
            }
            m_list.append(m_dict)
            
            
        for j in m_list:
            print(j['Sr No'],j['Product_Existence_Rating'],j['room_rent_rating'])
            

   
         
obj1 = HealthInsuranceModel('10 Lacs',23)
# obj1.CoverPlan()
obj1.model_data()

        
        
