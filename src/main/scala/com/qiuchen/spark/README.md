部署说明：
作业需打包并上传到spark服务器上，并提交作业，以下面的命令为例：
spark-submit --master yarn \
--deploy-mode cluster 
--class com.qiuchen.spark.WordCount     (类名)
/opt/spark-study-1.0-SNAPSHOT.jar words (jar文件 参数)