from pyspark import SparkConf,SparkContext
import json
import re
import math
from pyspark.sql import SQLContext

# dat = spark.read.json("merged.json")
dat = spark.read.json("merged2.json")

# dat = dat.withColumn('Total_Crimes',dat.Total_Crimes.cast('int'))
# dat = dat.withColumn('Population',dat.Population.cast('int'))
# dat = dat.withColumn('Unemployment_Rate',dat.Unemployment_Rate.cast('float'))
# dat = dat.withColumn('Median_Household_Income',dat.Unemployment_Rate.cast('int'))
# dat = dat.withColumn('violent_crimes',dat.Unemployment_Rate.cast('float'))
# dat = dat.withColumn('murder',dat.Unemployment_Rate.cast('float'))



dat.registerTempTable('merged')

# (1) Spark MapReduce
#collect state-wise total crime rate, Unemployment_Rate, etc
#Sort the states by the state-wise total crime rate from highest to lowest
dat_rdd = dat.rdd

a2 =dat_rdd.map(lambda x:(x['State'],((int(x['violent_crimes']) + int(x['murder']))/int(x['Population']), 1)))
a3 = a2.reduceByKey(lambda x,y:((x[0]+y[0]),(x[1] + y[1])))
a3 = a3.filter(lambda x:x[1][0] > 0)
a4 = a3.map(lambda x:(x[0],x[1][0]/x[1][1])).sortBy(lambda x:x[1],ascending = False)

#[('CA', 0.003418678885950463), ('DE', 0.002347207896253163), ('LA', 0.0023158959679670647), ('NM', 0.0020646944056758676), ('TN', 0.0020134977827201495), ('AK', 0.0019270231406514418), ('MD', 0.0018345368202468987), ('NV', 0.0017993881925583437), ('AL', 0.0017240772735987065), ('NC', 0.0017158539903264786), ('SC', 0.0016515078047360057), ('MA', 0.0016239923782709466), ('WV', 0.0015380620351653856), ('AZ', 0.0015116650516028111), ('AR', 0.0014755299608766675), ('MO', 0.0014430769472764274), ('HI', 0.0013550335058973826), ('GA', 0.0013063486322366872), ('NY', 0.001276998924602213), ('MS', 0.001245330929190582), ('TX', 0.0012433567891632197), ('IN', 0.0012122237165717695), ('NJ', 0.001136867609812386), ('IA', 0.0011315549119471557), ('PA', 0.0011209760242846627), ('OR', 0.0010431153616132906), ('MI', 0.0010288786095420035), ('WI', 0.0010182326947480004), ('MT', 0.0009711003524874607), ('SD', 0.0009254428138348191), ('WA', 0.0009248217133457448), ('UT', 0.0009100785787837355), ('VA', 0.0008856924759250546), ('CO', 0.0008834850299503913), ('ID', 0.0008685193244313105), ('WY', 0.0008520285184677218), ('KS', 0.0007565208692529349), ('VT', 0.0007352178602200818), ('OK', 0.0007348936256652119), ('CT', 0.0007147292753940591), ('NH', 0.0006475796314504395), ('MN', 0.0005982347490309716), ('RI', 0.0005705508624092038), ('OH', 0.0005581215067820077), ('ME', 0.0005533160646104296), ('ND', 0.0005407962567726578), ('NE', 0.0005176100200634347), ('KY', 0.0004252690989628045), ('DC', 0.0002464221400815089)]



# (2) SparkSQL
#Using sparksql, calculate the average crime rate and group by state, and map it against
#unemployment rate and household income for each state. 

q00 =  sqlContext.sql("select State,avg(Crimes_Rate) as avg_crimes from merged group by State having avg_crimes <> 0 order by avg_crimes desc limit 10")
q10 =  sqlContext.sql("select State,avg(Unemployment_Rate) as avg_unemploy from merged group by State having avg_unemploy <> 0 order by avg_unemploy desc limit 10")
#Top 10 States with Highest Crime Rate: TN, NM, MD, MS, UT, WI, HI, WY, AZ, NV
#Top 10 States with Highest Unemployment Rate: AK, AZ, NM, WV, MS, CA, LA, AL, WA, KY


q11 =  sqlContext.sql("select State,avg(Crimes_Rate) as avg_crimes from merged group by State having avg_crimes <> 0 order by avg_crimes asc limit 10")
q12 =  sqlContext.sql("select State,avg(Unemployment_Rate) as avg_unemploy from merged group by State having avg_unemploy <> 0 order by avg_unemploy asc limit 10")
#Top 10 States with Lowest Crime Rate: DC, VT, MN, CT, OH, MA, OK, MT, NE, IA
#Top 10 States with Lowest Unemployment Rate: NH, NE, HI, CO, SD, ND, VT, IA, KS, ID


q20 =  sqlContext.sql("select State,avg(Crimes_Rate) as avg_crimes from merged group by State having avg_crimes <> 0 order by avg_crimes desc limit 10")
q21 =  sqlContext.sql("select State,avg(Median_Household_Income) as avg_income from merged group by State having avg_income <> 0 order by avg_income asc limit 10")
#Top 10 States with Highest Crime Rate: TN, NM, MD, MS, UT, WI, HI, WY, AZ, NV
#Top 10 States with Lowest Househould Income: MS, AR, AL, WV, NM, KY, LA, SC, TN, MO


q30 =  sqlContext.sql("select State,avg(Crimes_Rate) as avg_crimes from merged group by State having avg_crimes <> 0 order by avg_crimes asc limit 10")
q31 =  sqlContext.sql("select State,avg(Median_Household_Income) as avg_income from merged group by State having avg_income <> 0 order by avg_income desc limit 10")
#Top 10 States with Lowest Crime Rate: DC, VT, MN, CT, OH, MA, OK, MT, NE, IA
#Top 10 States with Highest House Income: NJ, CT, DC, MD, MA, RI, NH, HI, CA, AK





# (3) SparkSQL Job 2
#Drug 
q40 =  sqlContext.sql("select State,AVG(drugs_rate) as avg_drug from merged group by State order by avg_drug desc limit 10")
#NH has the highest drug rate, followed by MD, TN, CA, NJ, WY, UT, NV, SC, TX.


q41 =  sqlContext.sql("select State,sum(drugs) as sum_drug, sum(violent_crimes) as sum_violent, sum(property_crimes) as sum_prop, sum(assult) as sum_assult, sum(robbery) as sum_rob, sum(murder) as sum_murder,sum(fraud) as sum_fraud from merged group by State")
q41.registerTempTable("temp")
#violent_crimes	property_crimes	assult	robbery	murder	fraud	drugs
q42 =  sqlContext.sql("select State from temp where sum_drug > sum_violent and sum_drug > sum_prop and sum_drug > sum_assult and sum_drug > sum_rob and sum_drug > sum_murder and sum_drug > sum_fraud")
#[Row(State='SC'), Row(State='LA'), Row(State='NJ'), Row(State='VA'), Row(State='KY'), Row(State='WY'), 
# Row(State='NH'), Row(State='MI'), Row(State='WI'), Row(State='ID'), Row(State='CA'), Row(State='NE'), 
# Row(State='MD'), Row(State='MO'), Row(State='ME'), Row(State='ND'), Row(State='MS'), Row(State='IN'), 
# Row(State='OH'), Row(State='TN'), Row(State='PA'), Row(State='SD'), Row(State='NY'), Row(State='TX'), 
# Row(State='WV'), Row(State='GA'), Row(State='AR'), Row(State='OK'), Row(State='UT')]
