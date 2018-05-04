sudo rm -rf ~/work/shared/*
sudo cp /home/mqp29/workspace/streaming-app/target/streaming-app-1.0.1-SNAPSHOT.jar ~/work/shared/ 
sudo chmod 777 -R /home/mqp29/work/shared/

CMD="spark2-submit --name Mahendran --class com.icc.poc.StreamingSQLApplication --conf spark.yarn.submit.waitAppCompletion=false --master yarn --deploy-mode cluster --queue edhops /work/shared/streaming-app-1.0.1-SNAPSHOT.jar"

echo $CMD
docker exec -u hdfs node1 $CMD
