import pyspark
import pytest
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


test_files = ["datanull.csv","final_data.csv","distinct_sports.csv","top_10_sport.csv","top_10_countries.csv","top_10_athlete.csv",
		"top_2012_gold.csv","oldest_gold_ath.csv","ath_mul_med.csv","teen_med_count.csv","corr.csv","ath_gold_class.csv",
		"lewis.csv","stat.csv","us_yearwise.csv","us_yearwise_percent.csv","med_by_age.csv","age_cat.csv","swim_avg_age.csv",
		"top10_gold_byage.csv","top_10_maxage.csv","usa_2012_swim_diff.csv","usa_2012_top_2.csv"]

format_dict = {
		"datanull.csv":['Athlete', 'Age', 'Country', 'Year', 'Sport', 'GoldMedals', 'SilverMedals', 'BronzeMedals', 'TotalMedals'],	
		"final_data.csv":['Athlete', 'Age', 'Country', 'Year', 'Sport', 'GoldMedals', 'SilverMedals', 'BronzeMedals', 'TotalMedals'],		
		"distinct_sports.csv":["Sport"],
		"top_10_sport.csv":["Sport","TotalMedals"],
		"top_10_countries.csv":["Country","TotalMedals"],
		"top_10_athlete.csv":["Athlete","TotalMedals"],
		"top_2012_gold.csv":["Country","GoldMedals"],
		"oldest_gold_ath.csv":["Athlete"],
		"ath_mul_med.csv":["Athlete","Sport"],
		"teen_med_count.csv":["count"],
		"corr.csv":["correlation"],
		"ath_gold_class.csv":["title","count"],
		"lewis.csv":["Athlete"],
		"stat.csv":["avgAge","minAge","maxAge"],
		"us_yearwise.csv":["Country","Year","GoldMedals","SilverMedals","BronzeMedals","TotalMedals"],
		"us_yearwise_percent.csv":["Country","Year","PercentGold","PercentSilver","PercentBronze"],
		"med_by_age.csv":["ageCat","TotalMedals"],
		"age_cat.csv":["Sport","avgAge"],
		"swim_avg_age.csv":["Sport","Country","avgAge"],
		"top10_gold_byage.csv":["Country","teens","twenties","thirties","forties","fifties","sixties","Total"],
		"top_10_maxage.csv":["Sport","teens","twenties","thirties","forties","fifties"],
		"usa_2012_swim_diff.csv":["Athlete","TotalMedals","Medal_diff"],
		"usa_2012_top_2.csv":["Sport","Athlete","Age","TotalMedals"]}


exist = []
not_exist = []
bad_format = []


def test_file_exist_hdfs():
	#check if file is available in hdfs. if yes, call check_file_format to validate the header information. 
	for i in range(len(test_files)):
		try:
			rdd = spark.sparkContext.textFile(test_files[i])
			rdd.take(1)
			exist.append(test_files[i])
		except Py4JJavaError as e:
			not_exist.append(test_files[i])

	assert not not_exist, "Oops! looks like one or more files required for scoring are missing in HDFS. If you think they have been created, please recheck whether the file name matches what is given in the instruction including .csv. Note that you will still be able to submit for final scoring, but only the available files will pass through scoring. The files we think missing are: {}".format(not_exist)
	
@pytest.fixture
def exist_files():
    return exist

def test_check_file_format(exist_files):
	for file in exist_files:
		expected_cols = format_dict.get(file)
		if expected_cols == None:
			print("File format not defined")
		else:
			data = spark.read.csv(file, header=True, inferSchema=True)
			actual_cols = []
			[actual_cols.append(data.dtypes[i][0]) for i in range(len(data.dtypes))]
			if expected_cols != actual_cols:
				bad_format.append([file,expected_cols,actual_cols])
			

	assert not bad_format, "Looks like few output csv files do not have expected header column names. They need to match the expected column names as given in the instruction. Given below are the files we think are having header format issues. Read in order - [file name, expected cols, actual cols] {}".format(bad_format)



