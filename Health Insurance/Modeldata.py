import pandas as pd
import openpyxl as px
import psycopg2 as pg
import decimal

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
        cr.execute('''SET client_encoding TO 'utf8';''')
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
            # if len(df_age) != 1:         
            #     if self.age >= float(df_age[0]) and self.age <= float(df_age[1]):
            #         flag = True
            # else:            
            #     if self.age == float(df_age[0]):
            #         flag = True
            
            if len(df_age) != 1 and self.age >= float(df_age[0]) and self.age <= float(df_age[1]):         
                
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
            return (f_val)
  
        except (Exception, pg.Error) as error:
                print("Failed to insert record into fetched table", error) 
                return None         
        # for i in f_val:
        #     print(i[0],i[1],i[2])
        # print(f_val)
        # return (f_val)
        # return [('')]
    
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
            # if len(year_range) == 1:
            #     if int(year_range[0][1:]) <= brand_existence:
            #         flag = True
            #         rating = data[i][1]
            # elif len(year_range) == 2:                
            #     if brand_existence >= int(year_range[0]) and  brand_existence <= int(year_range[1]):
            #         flag = True
            #         rating = data[i][1]
            
            if len(year_range) == 1 and int(year_range[0][1:]) <= brand_existence:
                flag = True
                rating = data[i][1]
            elif len(year_range) == 2 and brand_existence >= int(year_range[0]) and  brand_existence <= int(year_range[1]):                
                flag = True
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
            
            # if len(year_range) == 1:
            #     if int(year_range[0][1:]) <= product_existence:
            #         flag = True
            #         rating = data[i][1]
            # elif len(year_range) == 2:                
            #     if product_existence >= int(year_range[0]) and  product_existence <= int(year_range[1]):
            #         flag = True
            #         # return data[i][1]
            #         rating = data[i][1]
            
            if len(year_range) == 1 and int(year_range[0][1:]) <= product_existence:
                
                flag = True
                rating = data[i][1]
            elif len(year_range) == 2 and product_existence >= int(year_range[0]) and  product_existence <= int(year_range[1]):                

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
        
     # //*------------Price Rating Data----------------------*//
    def Price_rating(self,sr_no):
        db = self.dbConnect()
        cr = db.cursor()  

        sql = '''select sr_no,pricing from fetcheddata order by pricing desc'''
        cr.execute(sql)
        data = cr.fetchall()        
        db.commit() 
        db.close()

        data_len = len(data)
        # print(data_len)
        for i in range (0,data_len):
            perct = round(((i+1)/data_len)*100)
            
            # print(perct)
            pr_rat = list(data[i])
            pr_rat.append(perct)
            
            if sr_no == pr_rat[0]:
                if pr_rat[2] < 20 :
                    return 1
                elif pr_rat[2] < 40:
                    return 2
                elif pr_rat[2] < 60:
                    return 3
                elif pr_rat[2] < 80:
                    return 4
                elif pr_rat[2] <= 100:
                   return 5
                else:
                    return 'Not Available'
            

            
        # return data
        
     # //*------------No claiM bonus Data----------------------*//
    def no_claim_bonus(self,keys):
        db = self.dbConnect()
        cr = db.cursor() 
        # cr.execute('Select ratings from ncb where keywords ='+"'"+ keys+"';")
        cr.execute('Select ratings,keywords from ncb;')
        data = cr.fetchall()
        db.commit()
        db.close()
        
        for i in range(len(data)):
            # print(data[i][1])
            if data[i][1].lower() == str(keys).lower():
                return data[i][0]
            
        return 'Not_Available'
    
    # //*------------Recharge Of Si Data----------------------*//
    def recharge_of_si(self,sr_no,keys):
        db = self.dbConnect()
        cr = db.cursor() 
        keys = str(keys).strip()
        cr.execute('Select ratings,keywords from recharge_of_si;')
        data = cr.fetchall()
        db.commit()
        db.close()
        
        for i in range(len(data)):
            if data[i][1].lower() == keys.lower():
                return data[i][0]  
        return 1
    
    # //*---------------Pre_Existing Data-------------------*//
    def pre_existing_disease(self,sr_no,keys):
        db = self.dbConnect()
        cr = db.cursor() 
        keys = str(keys).strip()
        cr.execute('Select ratings,keywords from pre_existing_disease;')
        data = cr.fetchall()
        # print(data)
        db.commit()
        db.close()
        
        for i in range(len(data)):
            if data[i][1].lower() == keys.lower():
                # print(data[i][0])
                return data[i][0] 

        return 'Not_Available'
    
    # //*---------------CO PAY Data-------------------*//
    def co_pay_ranking(self,sr_no,keys):
        db = self.dbConnect()
        cr = db.cursor() 
        keys = str(keys).strip()
        cr.execute('Select ratings,keywords from co_pay')
        data = cr.fetchall()
        # print(data)
        db.commit()
        db.close()
        
        for i in range(len(data)):
            if data[i][1].lower() == keys.lower():
                return data[i][0] 

        return 'Not_Available'
    
        # //*---------------Health and Wellness-------------------*//
    def health_wellness(self,sr_no,uniq_code):
        db = self.dbConnect()
        cr = db.cursor() 
        tpl = (uniq_code.strip(),)
        sql = '''Select ratings from health_wellness where unique_code_plan = %s ;'''
        cr.execute(sql,tpl)
        data = cr.fetchone()
        db.commit()
        db.close()
        
        if data:
            return data[0]
        else:
            return 'Not_Available'
        
      # //*------------Claim Settlement Ratio Rating Data----------------------*//
    def csr_rating(self,sr_no):
        db = self.dbConnect()
        cr = db.cursor()  

        sql = ''' select sr_no,"Claims_Settlement_Ratio (%)" , PERCENT_Rank() Over (order by "Claims_Settlement_Ratio (%)" desc)*100 from fetcheddata;'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()
        percent = 0
        
        for i in data:
            if sr_no == i[0]:
                percent = round(i[-1])
  
        if percent < 20 :
            return 5
        elif percent < 40 and percent >= 20:
            return 4
        elif percent < 60 and percent <= 40:
            return 3
        elif percent < 80 and percent >= 60:
            return 2
        elif percent <= 100 and percent >= 80:
            return 1
        else:
            return 'Not Available'
      
     # //*------------ICR Rating----------------------*//  
    def ICR_Rating(self,sr_no,icr_percet):
        db = self.dbConnect()
        cr = db.cursor()  
        sql = '''select incurred_claim_ratio, rating from rational_rating;'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()
        
        # print(data)
        
        for i in range(len(data)):
            # print(data[i][0])
            perct_range = data[i][0].split('-')
            # print(len(perct_range))
            if len(perct_range) == 1 and perct_range[0][0] == '>' and int(perct_range[0][1:]) <=  icr_percet:
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '<' and int(perct_range[0][1:]) >=  icr_percet:
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '=' and int(perct_range[0][1:]) ==  icr_percet:
                return data[i][1]
            elif len(perct_range) == 2 and int(perct_range[0]) <= icr_percet  and int(perct_range[1]) >= icr_percet:
                return data[i][1]
    
                
     # //*------------AOC Rating----------------------*//  
    def aoc_rating(self,sr_no,aoc_percet):
        db = self.dbConnect()
        cr = db.cursor()  
        sql = '''select "Ageing_of_Claim (%)", rating from rational_rating;'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()

        
        for i in range(len(data)):
            perct_range = data[i][0].split('-')
            if len(perct_range) == 1 and perct_range[0][0] == '>' and int(perct_range[0][1:]) <=  aoc_percet:
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '<' and int(perct_range[0][1:]) >=  aoc_percet:
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '=' and int(perct_range[0][1:]) ==  aoc_percet:
                return data[i][1]
            elif len(perct_range) == 2 and int(perct_range[0]) <= aoc_percet  and int(perct_range[1]) >= aoc_percet:
                return data[i][1]

         # //*------------Network Hospital Rating----------------------*//  
    def network_hospital(self,sr_no,nw_value):
        db = self.dbConnect()
        cr = db.cursor()  
        sql = '''select network_hospitals, rating from rational_rating;'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()
        # print(data)
        
        # print(data)
        
        for i in range(len(data)):
            # print(type(nw_value))
            # print(data[i][0])
            perct_range = data[i][0].split('-')
            
            if nw_value == 'Not_Available':
                return 1
            # print(len(perct_range))
            if len(perct_range) == 1 and perct_range[0][0] == '>' and int(perct_range[0][1:]) <=  float(nw_value):
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '<' and int(perct_range[0][1:]) >=  float(nw_value):
                return data[i][1]
            elif len(perct_range) == 1 and perct_range[0][0] == '=' and int(perct_range[0][1:]) ==  float(nw_value):
                return data[i][1]
            elif len(perct_range) == 2 and int(perct_range[0]) <= float(nw_value)  and int(perct_range[1]) >= float(nw_value):
                
                return data[i][1]
            
            
    # //*------------avg_product_features----------------------*// 
    def avg_product_features(self,room_rent,ncb,recharge_of_si,pre_existing_disease,co_pay,health_wellness):
        
        db = self.dbConnect()
        cr = db.cursor()  
        sql = '''select "Weightage (%)" from weightage where parameters IN ('Room Rent', 'NCB','Recharge of SI','Pre-Existing Diseases','Co-pay','Health and Wellness');'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()
        # print(type(data[0][0]))
        # print(type(ncb))
        # print(room_rent,ncb,recharge_of_si,pre_existing_disease,co_pay,health_wellness)
        top = ((data[0][0] * decimal.Decimal(room_rent))+(data[1][0] * decimal.Decimal(ncb))+ (data[2][0] * decimal.Decimal(recharge_of_si))+(data[3][0] * decimal.Decimal(pre_existing_disease))+(data[4][0] * decimal.Decimal(co_pay)) + (data[5][0] * decimal.Decimal(health_wellness)))
        bottom = ((data[0][0])+(data[1][0])+(data[2][0])+(data[3][0])+(data[4][0])+(data[5][0]))
        
        val = top/bottom
        return float('{:.1f}'.format(val))
        # return round(val)
    
     # //*------------avg_product_features----------------------*// 
    def avg_CSE_features(self,csr_rating,icr_rating,aoc_rating,network_hospital):
        
        db = self.dbConnect()
        cr = db.cursor()  
        sql = '''select "Weightage (%)" from weightage where parameters IN ('Claim Settlement Ratio','Incurred Claim Ratio','Age Analysis of No. of Claims Paid(%)<3months','Network Hospitals');'''
        cr.execute(sql)
        data = cr.fetchall()  
        db.commit()
        db.close()
        # print(type(data[0][0]))
        # print(type(ncb))
        print(csr_rating,icr_rating,aoc_rating,network_hospital)
        top = ((data[0][0] * decimal.Decimal(csr_rating))+(data[1][0] * decimal.Decimal(icr_rating))+ (data[2][0] * decimal.Decimal(aoc_rating))+(data[3][0] * decimal.Decimal(network_hospital)))
        bottom = ((data[0][0])+(data[1][0])+(data[2][0])+(data[3][0]))
        
        val = top/bottom
        return float('{:.1f}'.format(val))
        # return round(val)
        
    # //*------------Final Model Data----------------------*//    
    def model_data(self):
        f_data = self.CoverPlan()
        if f_data is  None:
            return print("No Value")
        
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
                room_rent = 5
            else:
                room_rent = room_rent[0]


            price_rat = self.Price_rating(sr_no)
            ncb = self.no_claim_bonus(i[10])
            recharge_of_si = self.recharge_of_si(sr_no,i[11])
            pre_existing_disease = self.pre_existing_disease(sr_no,i[12])
            co_pay =  self.co_pay_ranking(sr_no,i[13])
            health_wellness = self.health_wellness(sr_no,i[4])
            csr_rating = self.csr_rating(sr_no)
            icr_rating = self.ICR_Rating(sr_no,i[16])
            aoc_rating = self.aoc_rating(sr_no,i[17])
            network_hospital = self.network_hospital(sr_no,i[18])
            avg_brand_existance = brand_existance_rating
            avg_product_existance = product_existance_rating
            avg_price_rating = price_rat
            avg_product_features = self.avg_product_features(room_rent,ncb,recharge_of_si,pre_existing_disease,co_pay,health_wellness)
            avg_CSE_features = self.avg_CSE_features(csr_rating,icr_rating,aoc_rating,network_hospital)
            print(i_plan,i[18],avg_CSE_features)

            
            
            m_dict = {
                'Sr No' : sr_no,
                'Insurer_Name' : i_name,
                'Insurance_Plan' : i_plan,
                'Age Range' : age_range,
                'Cover Plan' : cover_plan,
                'Brand Existance Rating' : brand_existance_rating,
                'Product_Existence_Rating' : product_existance_rating,
                'room_rent_rating' : room_rent,
                'price_rating' : price_rat,
                'ncb_rating' : ncb,
                'recharge_of_si' : recharge_of_si,
                'pre_existing_disease' : pre_existing_disease,
                'co_pay' : co_pay,
                'health_wellness' : health_wellness,
                'csr_rating' : csr_rating,
                'icr_rating' : icr_rating,
                'aoc_rating' : aoc_rating,
                'network_hospital' : network_hospital,
                'avg_brand_existance' : avg_brand_existance,
                'avg_product_existance' : avg_product_existance,
                
                
                
            }
            m_list.append(m_dict)
            # print(len(m_list))
            # print(m_dict)
            
        # for j in m_list:
        #     print(j['Sr No'],j['Insurer_Name'],j['room_rent_rating'],j['icr_rating'])
    
    
                


   
         
obj1 = HealthInsuranceModel('5 Lacs',23)
# obj1.CoverPlan()
obj1.model_data()

        
        
