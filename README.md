# CodingKataExercise

#Run Using Submit Command

**Run As-> maven build --> clean package // It will generate Jar

//Submit Command
**spark-submit --repositories http://repo.hortonworks.com/content/groups/public/,http://repo1.maven.org/maven2/ --class com.employee.report.EmployeeReport --executor-memory 2g --driver-memory 2g --num-executors 3 --executor-cores 2 /home/sshuser/sud_jar/EmployeeReports-0.0.1-SNAPSHOT.jar /azuser/user/sudData/maxsalaryOutput/ /azuser/user/sudData/emp_dept_data/load_titles.csv /azuser/user/sudData/best10PaidEmployeeOutputPath/ /azuser/user/sudData/emp_dept_data/load_employees.csv /azuser/user/sudData/emp_dept_data/load_dept_manager.csv /azuser/user/sudData/notManagerOutputPath/

** Specify jar location and argument according to cluster

**val maxsalaryOutput = args(0) //Max Salary Output Path
    val titlesInputPath = args(1) //Job Title Input Path
    val best10PaidEmployeeOutputPath = args(2) // Employee title file Path
    val employeeInputPath = args(3) // Employee details Input Path
    val managerInputPath = args(4) // Manager details Input Path
    val notManagerOutputPath = args(5) // Output location for not manager employee
**Change path of all salaries file
    val salariesPaths = List("/azuser/user/sudData/emp_dept_data/load_salaries1.csv","/azuser/user/sudData/emp_dept_data/load_salaries2.csv","/azuser/user/sudData/emp_dept_data/load_salaries3.csv")
    
#Run In Local Eclipse

** Import maven project in eclipse
** All input and output available in resource path \src\test\resources 
<!-- uncomment line 14 which is setup to run locally spark session -->
<!-- comment line 11 which is setup to run from submit command -->

**Note: In project Build Path -> Configure Build Path -> Add scala libraries container and save before running

# For Avro format there may be version discrepancy

**Specify package in submit command i.e spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.4
**Uncomment save avro File

#Note: Sample output is stored in \src\test\resources to verify output