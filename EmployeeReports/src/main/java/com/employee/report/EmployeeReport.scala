package com.employee.report
import org.apache.spark.sql.SparkSession

object EmployeeReport {
  
  def main(args:Array[String]) {
    val appname = "EmployeeReportsKataExercise:"
    
    
    //Starting Spark Session
    val spark = SparkSession.builder().appName(appname).getOrCreate()
    
    //Uncomment to run localally
    //val spark = SparkSession.builder().appName(appname).master("local").getOrCreate()
    
    //List of argument and path
    val maxsalaryOutput = args(0)
    val titlesInputPath = args(1)
    val best10PaidEmployeeOutputPath = args(2)
    val employeeInputPath = args(3)
    val managerInputPath = args(4)
    val notManagerOutputPath = args(5)
    //Paas path of all salaries file
    val salariesPaths = List("/azuser/user/sudData/emp_dept_data/load_salaries1.csv","/azuser/user/sudData/emp_dept_data/load_salaries2.csv","/azuser/user/sudData/emp_dept_data/load_salaries3.csv")
    
    /*//Local Path Specify
    val maxsalaryOutput = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\maxsalaryOutput\\"
    val titlesInputPath = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_titles.csv"
    val best10PaidEmployeeOutputPath = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\best10PaidEmployeeOutputPath"
    val employeeInputPath = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_employees.csv"
    val managerInputPath = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_dept_manager.csv"
    val notManagerOutputPath = "C:\\CEP\\EmployeeReports\\src\\test\\resources\\notManagerOutputPath"
    val salariesPaths = List("C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_salaries1.csv","C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_salaries2.csv","C:\\CEP\\EmployeeReports\\src\\test\\resources\\emp_dept_data\\load_salaries3.csv")
    */
    
    //Getting all salaries in single DataFrame
    val rawDF = spark.read.option("header", "true").csv(salariesPaths:_*).toDF
    
    //Deduplicate records if any and casted salary to DoubleType
    import org.apache.spark.sql.expressions.Window
    import spark.implicits._
    val windowDedup = Window.partitionBy($"emp_no",$"salary").orderBy(($"salary").desc)
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    val dedupRawDF = rawDF.select($"emp_no", $"salary".cast(DoubleType), row_number().over(windowDedup).as("rowNum")).where("rowNum < 2").select($"emp_no", $"salary")
    
    //case 1: All employees and their max salary
    @transient val maxWindow = Window.partitionBy($"emp_no")
    val dedupRawGroupByDF = dedupRawDF.withColumn("max_salary", max($"salary").over(maxWindow))
    val max_salaryDF = dedupRawGroupByDF.select("*").where($"salary"===$"max_salary").drop("salary")
    
    //Store max salary in avro format
    import org.apache.spark.sql.SaveMode
    max_salaryDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("delimiter", ",").option("header", "true").save(maxsalaryOutput)
    //max_salaryDF.write.format("avro").save(maxsalaryOutput)
    
    //case 2: Job titles of the best paid 10 employees in the company
    val bestPaidEmployeeDF = max_salaryDF.orderBy(($"salary").desc).limit(10)
    val titlesDF = spark.read.option("header", "true").csv(titlesInputPath).toDF
    val best10PaidEmployeeTitles = bestPaidEmployeeDF.join(titlesDF,"emp_no").select($"title").distinct
    best10PaidEmployeeTitles.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("delimiter", ",").option("header", "true").save(best10PaidEmployeeOutputPath)
    //best10PaidEmployeeTitles.coalesce(1).write.mode(SaveMode.Overwrite).format("avro").option("delimiter", ",").option("header", "true").save(best10PaidEmployeeOutputPath)
    
    //case 3: All employees who were never managers
    val employeeDF = spark.read.option("header", "true").csv(employeeInputPath).toDF.select($"emp_no")
    val managerDF = spark.read.option("header", "true").csv(managerInputPath).toDF.select($"emp_no")
    val notManagerDF = employeeDF.except(managerDF)
    notManagerDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("delimiter", ",").option("header", "true").save(notManagerOutputPath)
    //notManagerDF.coalesce(1).write.mode(SaveMode.Overwrite).format("avro").option("delimiter", ",").option("header", "true").save(notManagerOutputPath)
    
    spark.close()
  }
}