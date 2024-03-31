from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, min, max

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("olympics-analysis").getOrCreate()

# Đọc dữ liệu từ các file CSV và tạo DataFrame trong PySpark
vdv_olympics_data = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_data = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)
# 3.1. Hiển thị schema và số bản ghi của hai bộ dữ liệu
print("Schema của bộ dữ liệu vdv_olympics:")
vdv_olympics_data.printSchema()
print("Schema của bộ dữ liệu qg_noc:")
qg_noc_data.printSchema()
num_records_vdv_olympics = vdv_olympics_data.count()
print("Số bản ghi trong bộ dữ liệu vdv_olympics là:", num_records_vdv_olympics)
num_records_qg_noc = qg_noc_data.count()
print("Số bản ghi trong bộ dữ liệu qg_noc là:", num_records_qg_noc)

# 3.2. Thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000
vietnam_athletes_2000 = vdv_olympics_data.filter((col('NOC') == 'VIE') & (col('Year') >= 2000) & (col('Year') < 2010))
print("Thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000:")
vietnam_athletes_2000.show()
# 3.3. Thông tin về 7 quốc gia có số lượng vận động viên đông nhất tham gia thế vận hội 2016
top_countries_athletes_2016 = vdv_olympics_data.filter(vdv_olympics_data['Year'] == 2016).groupBy('NOC').agg(count('*').alias('athletes_count')).orderBy('athletes_count', ascending=False).limit(7)
print("Thông tin về 7 quốc gia có số lượng vận động viên đông nhất tham gia thế vận hội 2016:")
top_countries_athletes_2016.show()

# Câu 3.5: Số lượng vận động viên nam tham gia mỗi kỳ Thế vận hội trong thế kỷ 21
male_athletes_by_year = vdv_olympics_data.filter(col('Sex') == 'M').groupBy('Year').agg(count('*').alias('number_of_athletes'))
print("Số lượng vận động viên nam tham gia mỗi kỳ Thế vận hội trong thế kỷ 21:")
male_athletes_by_year.show()

# 3.6. Tổng số vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga
russia_athletes_1990s = vdv_olympics_data.filter((col('NOC') == 'RUS') & (col('Year') >= 1990) & (col('Year') < 2000)).groupBy('Year').agg(count('*').alias('number_of_athletes'))
print("Tổng số vận động viên tại mỗi kỳ Thế vận hội trong thập kỷ 1990 của Nga:")
russia_athletes_1990s.show()

# 3.7. Chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia Thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần
athlete_height_stats = vdv_olympics_data.groupby('NOC').agg(min('Height').alias('min_height'), mean('Height').alias('avg_height'), max('Height').alias('max_height')).orderBy('max_height', ascending=False)
print("Chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia Thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần:")
athlete_height_stats.show()

# 3.8. Tên các quốc gia không có thông tin về quốc gia trong qg_noc
missing_country_info = qg_noc_data.filter(qg_noc_data['Population'].isNull())
print("Tên các quốc gia không có thông tin về quốc gia trong qg_noc:")
missing_country_info.show()

# 3.9. Danh sách 10 quốc gia giành nhiều huy chương vàng nhất Thế vận hội mùa hè 2012
top_10_gold_medal_countries_2012 = vdv_olympics_data.filter((col('Year') == 2012) & (col('Season') == 'Summer') & (col('Medal') == 'Gold')).groupBy('NOC').agg(count('*').alias('gold_medals_count')).orderBy('gold_medals_count', ascending=False).limit(10)
print("Danh sách 10 quốc gia giành nhiều huy chương vàng nhất")
top_10_gold_medal_countries_2012.show()

# 3.10. Danh sách 10 vận động viên giành nhiều huy chương vàng nhất thế kỷ 20
_10_athletes_20th_century = vdv_olympics_data.filter((col('Year') >= 1900) & (col('Year') <= 2000) & (col('Medal') == 'Gold')).groupBy('Name').agg(count('*').alias('gold_medals_count')).orderBy('gold_medals_count', ascending=False).limit(10)
print("Danh sách 10 vận động viên giành nhiều huy chương vàng nhất thế kỷ 20:")
_10_athletes_20th_century.show()

# Câu 4: Lọc dữ liệu để chỉ bao gồm các vận động viên giành huy chương vàng
gold_medalists = vdv_olympics_data.filter((col('Year') == 2016) & (col('Medal') == 'Gold'))
print("Lọc và hiển thị thông tin về các vận động viên giành huy chương vàng từ dữ liệu vận động viên tại Thế vận hội 2016:")
gold_medalists.show()
