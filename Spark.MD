sql  => interview based qustions and task


spark is opensource
			-Distributed Execution Framework
			-Cluster computed wit
			-In-Memory computing with
			-Parallel process style
			-for large scale data processing

---------
spark + python = pyspark
-----------
why Pyspark

Pyspark is the Python API to use Spark

Pyspark is a lightening fast technology that is designed for fast computing

Pyspark is a demanding tool among the data engineers to work with bigger datasets

--------------
Pyspark used for
	-  Distributed SQL
	- Creating Data Pipelines
	- ingestion data into database
	- ML Algorithms
	- Graph or data streams
	
Pyspark packages:
		pyspark
		pyspark.sql
		pyspark.streaming
		pyspark.ml

-------------------------
* In-memory Computation : Storage and processing happening with the memory(ram) level
* Lazy Evaluation		: The dataflow gets executed only when we perform an action
* Fault Tolerant		: multiple tasks executed by multiple slaves(systems), if any one slave goes down then that task will be allowacted to another slave(system)	
* Persistence			: Trasformation can be re-used
* Distributed
* Parallel processing	

------------------------------------------
## RDDs(Resilient Distributed Datasets)
```
-Spark Data objects are called as RDDs

-RDDs are sub-divided into partitions

-These partitions are distributed across multiple RAMS of multiple slave nodes and are processed parallely

-RDD programming style is	"Dataflow"

-Dataflow is a sequence collection of pipes

-A pipe can be any data operation such as loading data, transforming data, filtering data, groupin data, merging data etc.

-In Spark ,Dataflow is executed in sequence .i.e RDD by RDD (one after the another) with in-memory computing feature and persistence fature.
```
### 2 RDDs cannot execute in parallel as one RDD o/p is the inpuit to the next RDD.

### But if a RDD has 20 partitions, then they can execute in parallel

# How to calculate the cluster memory in spark while executing spark Job.
----------------------------------------------------------------------

-For parallelism	-> we run the code in different machines instead of running in a single machine
```
spark 1.xxx------------>spark context
spark 2.xxx------------>spark session
```
when we submit a spark job then sparkcontext or sparksession intiates a JVM process to execute the entire job. This JVM process is called a executor

1 Node----> can have 1 or more exectors 
again inside the executor, we have to mention, how many cores we have

here cores means threads

Executor is JVM process, we need some threads to execute the tasks, these execution threads are called as cores.

* what is the ideal or optimal count of cores for each executor?? --> 4

--How to see the No of cores in a machine--> TAskManager-->Performance tab-->
		
RDD operations: 2 operations can be performed on a RDD
		Trasformations
		Actions
		
if Trasformations applied on RDD  ---> resultant also is a RDD(spark bject)
if actions applied on a RDD	  ---> resultant is a local object(python object)	

Q) How to get a spark object from a python object like list
  when we parallelize a python object(list) using spark context ---> we get spark object(RDD)
		
ex:
x=[10,20,30,40,50]
rdd1=sc.parallelize(x)
type(rdd1)
rdd1.collect()
		
------------
on a RDD, 2 types of operations can be performed

* Trasformation	
* Actions

# Trasformations : 3 Types
	Each element Trasformation ex: map(), flatmap()
	Filter Trasformation : filter, filter by range()
	Aggregated Trasformation: group by key()
	
# Actions: when an action is performed	on a RDD, then only the dataflow will be executed.
the following are some actions
```
	* collect () : It will collect all partitions data of different slave machines into client machine
					ex: RDD.collect()
	* count() 	 : Counts the no of elements in a  RDD
					RDD.count()
	* task()	 : Takes first 'n' of elements in a RDD.
					RDD.take(10)
	* save as text file() : To save the o/p to HDFS			
```		



## Fault Tolerance model in spark

# case 1: when current RDD machne is down		
```
				R1------------R2------------o/p
				
During the execution of current RDD(R2),if any one of the partition of current RDD is down, then all current RDD partition(p1,p2.p3 of R2) will be deallocated (clear from the RAM) and re-initiated or re-loaded in another slaves
		
-3 partitions are stored in 3 different machines excluding the machine which is down		
```	
```	
persistence flow
	
   r1  (i/p)
    |  (o/p)
   r2  (i/p)
    |  (o/P)
   r3  (i/p)            persistence
 /   |	\
r4  r5	r6
 |   |  |
r7  r8	r9
 |   |  |
r10 r11 r12


	
	
	flow -1 = r1 - r2 - r3 - r4 - r7 - r10
	flow -2 = r1 - r2 - r3 - r5 - r8 - r11
	flow -3 = r1 - r2 - r3 - r6 - r9 - r12

```

-----------------------------------

## RDD.getNumpartitions() --> to list the partition count
## RDD1 -> sc.textfile ("path/file.txt",10) --> to create the partition 

-------------------------
# case 2: If prevous RDD machine is down
```
R1------------R2------------R3	
S1	      S2	    S3	

DURING eXECUTION OF r3, WHILE TAKING i/p from R2 , if R2 failed then here R2 cant be reloaded or reinitiated in other machines becoz R2 i/p is R1 where R1 is not available in RAM, so the process should start from beginning or from persisted RDD 
```
-------------------------
# case 3: If a persisted RDD machine is down
```
R1------------R2------------R3------------R4------------R5	
S1	      S2            S3		  S4           S5
			persisted
							
R3 stored in S3 and persisted , but suddenly S3 is down						
then the flow will be re-executed from the beginning or from the available source
automatically till R3 without performing action and R3 will be re-persisted
```
-------------------------
# case 4: If s4 is down
```
R1------------R2------------R3------------R4------------R5	
S1	      S2            S3		  S4	        S5
			persisted
							
While Computing R5 and while taking i/p from R4, if R4 is failed 
then i/p is taken from R3 (which is available from RAM) and reloading the partitions from R4 in other machines and computing and provides i/p to R5
```
-------------------------


# case 5: can we persist 2 RDDs ?

```
R1------------R2------------R3------------R4------------R5------------R6	
S1	      S2            S3      	  S4            S5            S6
			persisted                   persisted
							
							
							
If R5 machine is down what happens?		
	Flow will re-executes from R3 to R5 and R5 will be re-persisted

If R3 machine is down, ?
	Flow will be re-executed from R1 to R3 automatically and R3 will be re-persisted

```
----------------------------------------------
Various spark in-built Transformations:
______________________________________

1.map()
2.flatmap()
3.filter()
4.union()
5.intersection()
6.sustract()		
7.cartesian()
8.distinct()


All the abouve Transformations can be applied onlyy on a RDD(spark data object)

1.map() : Each element Trasformation
		Applying an operaction on  each element of a RDD and returns a RDD.
			
EX: Squaring each element of a RDD

r1=sc.parallelize([10,20,30,40,50]) #parallelizing python object to spark object
re=r1.map(lambda x:x*x) #here x is each element of rdd(r1)
		#map transformation when applied on rdd r1 the reultant also is a rdd(r2)
r2.collect()	#here collect() is an action---> which returns python object
		#when action applied only, the dataflow will be executed
				
				
o/p:
[100, 400, 900 ,1600, 2500]

---------------------------------------------------------
2.flatMap(): flattens the elements of a list or a tuple
	    i.e flattens all the inner lists into one

x=[[10,20,30],[40,50,60]]
r1=sc.parallelize(x)
r2=r1.flatMap(lambda x:x)
re.collect()
[10,20,30,40,50,60]

ex:2
months=[["JAN',"Feb","March"],["April","May","June],["July","Aug,"Sep"]]
r1=sc.parallelize(months)
r2=r1.flatMap(lambda x:x)
r2.collect()
["JAN',"Feb","March","April","May","June,"July","Aug,"Sep"]
type(months)
type(r1)
type(r2)


Note: 	RDD2=RDD1.transformation()
		list=RDD1.action()

----------------------------------------------------------------------------
3.filter(): it filters the elements of a RDD based on condition and the resultant is also a RDD.

a=[10,20,30,40,50,60,70,80,90]
r1=sc.parallelize(a)
r2=r1.filter(lambda x:x>=30)
res1=r2.collect()
print(res1)
type(res1)	

a=["Java","Python","Java","Python","Python","Scala","Python"]
#I want the count of students perfering python
r1=sc.parallelize(a)
r2=r1.filter(lambda x:x=="Python")
r2.collect()
["Python","Python","Python","Python"]
res1=r2.collect()
print("No of students perfering Pythn is:", len(res1))

----------------------------------------------------------------------------
4. union() : combines elements of multiple RDDs
	by default performs union all operatons(Allows duplicates)

r1=sc.parallelize([10,20,30,40,50])
type(r1)
r2=sc.parallelize([10,20,60,70,80])
res=r1.union(r2)
res2=res.collect()
peint(res2)

----------------------------------------------------------------------------
5. intersection(): returns only the common elements

res1=r1.intersection(r2)
res1.collect()

----------------------------------------------------------------------------
6. Subtract(): removes common elements from a RDD

r3=r1.subtract(r2)
r2.collect()

----------------------------------------------------------------------------
7. cartesian(): performs cartesian product with other RDD
r1=sc.parallelize()[1,2,3])
r2=sc.parallelize()[4,5,6])
r3=r1.cartesian(r2)
res=r3.collect()
print(res)
----------------------------------------------------------------------------
8. distinct(): Remove the duplicates

r1=sc.parallelize([10,20,30,40,50,10,20,30])
r2=r1.distinct()
res5=r2.collect()
print(res5)


----------------------------------------------------------------------------
9. Transformations(in-built) on a pair RDDs ie(Key,value)pairs

1.  reduceByKey()
2.  groupByKey()
3.  sortByKey()
4.  mapvalues()
5.  key()
6.  values()
7.  join()
8.  leftOuterJoin()
9.  rightOuterJoin()
10. fullOuterJoin()

-------------------------
reduceByKey() : sums up all the values with same key reduceByKey can be applied only on (k,v) pair.

dnosals=[(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000)]				
r1=sc.parallelize(dnosals)
res=r1.reduceByKey(lambda x,y:x+y)
res.collect()

#It groups the similar keys and aggregates

ex:
r1=sc.parallelize([("Kohli",70),("Rohith",20),("Rahul",60),("Kohli",90),("Rohith",50),("Rahul",30)])
res=r1.reduceByKey(lamda x,y:x+y)
res.collect()

----------------------------
groupByKey() : In groupByKey(), we can get all the aggregations --> sum(),avg(),max(),min(),count() here we get a iterable object,convert to list and perform all operations

dnosals=[(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000),(11,70000),(12,80000)]
r1=sc.parallelize(dnosals)
res=r1.groupByKey()
res.collect()


Here we geta iterable object, which contains all the sals of each dept
ex:
11 <iterable object> ---> it contains all dno 11 salaries
12 <iterable object> ---> it contains all dno 12 salaries
13 <iterable object> ---> it contains all dno 13 salaries

convert each iterable object to list using map() and perform all aggergations

dnosals=[(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000),(11,70000),(12,80000)]
r1=sc.parallelize(dnosals)
res=r1.groupByKey()
res.collect()

res2=res.map(lambda x:(x[0],list(x[1])))
res2.collect()

res3=res2.map(lambda x:(x[0],sum(x[1]),max(x[1]),min(x[1]),len(x[1]),sum(x[1])/len(x[1])))
res3.collect()

----------------------------------------------------------------------------
3. sortByKey():sorting based on key

r1=sc.parallelize([(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000),(11,70000)])
res=r1.sortByKey()
res.collect()


----------------------------------------------------------------------------
4. mapValues() : Applying a functionality to eaach value, without changing the key

r1=sc.parallelize([(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000),(11,70000)])
res=r1.mapValues(lambda x:x+5000)
res.collect()

----------------------------------------------------------------------------	
5. key(): returns the keys of RDDs

r1=sc.parallelize([(11,10000),(12,20000),(13,30000),(11,40000),(12,50000),(13,60000),(11,70000)])
res=r1.keys()
res.collect()

----------------------------------------------------------------------------	
6. values(): returns the values of a RDD
r1=sc.parallelize([("Ram","Chennai"),("Abdul","Mumbai"),("Miller","Pune"),("Amar","Chennai")
res=r1.values()
res.collect()

----------------------------------------------------------------------------	
7. join():

r1=sc.parallelize([(10,20),(30,40),(50,60)])
r1=sc.parallelize([(10,20),(30,40),(70,80)])
ij=r1.join(r2)
ij.collect()

loj=r1.leftOuterJoin(r2)
loj.collect()

roj=r1.rightOuterJoin(r2)
roj.collect()

foj=r1.fullOuterJoin(r2)
foj.collect()

----------------------------------------------------------------------------	
Actions: whenever action is performed, the flow executes from its root RDD

The following are some of the actions

1.  collect()
2.  count()
3.  countByValue
4.  countByKey()
5.  take(num)
6.  top(num)
7.  first()
8.  reduce()
9.  sum()
10.  max()
11.  min()
12.  count()
13.  saveAsTextFile(path)

---------
1. collect(): It will collect all partitions data of different slave machines into client machine

x=[10,20,30,40,50,60]
r1=sc.parallelize(x)
r1.collect()

*************

2.count() : counts the no of elements in a RDD.
r1.count()

*************
3. countByValue(): counts no of times each value occurs in a RDD
					It return a dictionary

medals=["IND","AUS","ENG","IND","AUS","ENG","IND","AUS"]
r1=sc.parallelize(medals)
r1.countByValue()

*************
4. countByKey(): counts no of times each key has occured
				 we get o/p in the form of dictionary(key:value)
				 It shoud be applied only on a paired RDD.
				 
pairs=[('m',200000),('q',200000),('f',600000),('m',300000),('f',400000),('m',500000)]
r1=sc.parallelize(pairs)
r1.countByKey()

*************
5. take(n): takes first 'n' no of elements of a RDD.
r1=sc.parallelize([10,20,30,40,50])
r1.take(3)

*************
6. top(n): Take top 'n' elements of a RDD

r1.top(3)

EX:
names=sc.parallelize(["Rohith","Sharma","Archana","Megala","Kavi"])
name.take(2)
names.topp(2)

*************
7. first() : takes the 1st element of a RDD

names.first()

sals=[10000,30000,20000,50000,40000]
sals.top()

sals=[10000,30000,20000,50000,40000]
r1=sc.parallelize(sals)
r1.take(2)
r1.top(2)
r1.first()

*************
8. reduce() combines or sums elements of a RDD

x=[10,20,30,40,50]
r1=sc.parallelize(x)
r1.reduce(lambda x,y:x+y)

*************

9. sum() : finds the sum of RDD elements

r1.sum()


*************
10. max() : finds the max element of a RDD

r1.max()

*************
11. min() : finds the max element of a RDD

12.count(): counts the no of elements in a RDD

r1.min()

r1.count()

r1.sum()/r1.count()



---------------------------	
YARN (Yet-Another-Resource-Negotiator)
________________________________________

Before hadoop 2.xx version SPOF(Single-point-of-Failure) is at NameNode
i.e if NameNode is down then the entire cluster is down

so from hadoop 2.xx version onwards, it is SPOF-Free Arrchitecture

From hadoop 2.xxx version onwards, we got a feature ---> hight-Availability (HA)
i.e we have multiple NameNode

	NameNode1		NameNode2		NameNode3
	  Active		  Passive		  Passive
	  down			  Active		  Passive
					  down			  Active

At a point of time, only one NameNode will be active 					  
Passive NameNodes will keep collecting data from Active NameNode
Suddenly if Active nameNode is down, then anyone of he Passive NameNode will become Active.

Q) How NaneNod2 comes to know that NameNode1 is down?
-Based on the heart beat signals

-----------------------------------------------------------------------
Diff b/w hadoop 0.xx/1.xx version with hadoop 2.xx/later

		Hadoop 0.xx/1.xx									Hadoop 2.xx/later
		
Here SNN is not 100% backupnode,				 Here if NN1 is down, then immediately NN2 becomes active
here SNN cannot write or update to 				 here NN1,NN2 all can write or update into fsimage or edits		
fsimage and edits		

Here meta-dat sharing is not possible			Here meta-dat sharing is possible

Here there is no Hight-Availability(HA)			We have Hight-Availability(HA)

Here SPOF is at NameNode						here it is SPOF-Free Architecture
 
Here Default Blocksize=64mb						Here Default Blocksize=128mb

storage requesr:								storage requesr:
hadoop fs -put file1 /hdfsdir1					hdfs dfs -put file1 /hdfsdir1
		or												or
hadoop fs -commandname							hdfs dfs -commandname


Here huge burden of Job-Tracker as it has		Here the (JT) Job-Tracker is replaced by 2 componenets
to maintain the following						*) Resource manager(RM) ---> for resource mgm
*) Resource Management							*) Application Master(AM) --> for task Monitor
*) Task Monitoring									TT---> Nodemanager


Starting the services							Starting the services
start-dfs.sh-----> starting storagecomponents	start-dfs.sh-----> starting storagecomponents	
start-mapred.sh--> starting Processingcomponens	start-yarn.sh--> starting Processingcomponens	

---------------------
YARN Components ---> 7 components
----(1 to 4 Master level)--
1) Resource Manager    (RM)
2) Application Manager (AM)
3) Resourse Schedular  (RS)
4) Job History Server  (JHS)
----(5 to 7 Slave level)---
5) Application Master  (AM)
6) Node Manager        (NM)
7) Container

-------------------------------------

1) Resource Manager (RM): Responsible for Resource management
   Resource Manger has got 2 sub-services
		1) Application Manager (AM)  : Task related
		2) Resource	Schedular (RS)   : Resource related 

2) Job History server: keeps track of history of all he jobs

3) Application Master: Responsible for Task monitoring

for 1 Job   -> 1 AM is assigned
	3 Jobs  -> 3 AM is assigned
	10 Jobs -> 10 AM is assigned
	N Jobs  -> N AM is assigned

4) Node Manager(NM) : Responsible for executing the tasks 
	[ who executing task Node manager who assign the task resource manager]
	
5) Container: Set of H/W Resources requiredfor executing a task
