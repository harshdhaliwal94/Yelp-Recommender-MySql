DELIMITER //
CREATE PROCEDURE kmeans2(
    IN v_K int,
    IN training boolean)
BEGIN
DECLARE converge float;
DECLARE max_itr int(10) DEFAULT 1000000;
/*limit iterations 1 million*/
DECLARE itr_cnt int(10) DEFAULT 0;
DECLARE min_change float DEFAULT 0.001;

DROP TABLE IF EXISTS km_clusters_table2;
CREATE TABLE km_clusters_table2(
    id int auto_increment primary key,
    latitude float,
    longitude float,
    avg_star float
    );

-- initialize cluster centers
INSERT INTO km_clusters_table2 (latitude, longitude,avg_star) SELECT latitude, longitude,avg_star FROM kmeans_dataset2 LIMIT v_K;

set @max_itr:=1000000;
set @itr_cnt:=0;
set @min_change:=0.001;

REPEAT
   -- assign clusters to data points
   UPDATE kmeans_dataset2 d SET cluster_id = (SELECT id FROM km_clusters_table2 c
       ORDER BY POW(d.latitude-c.latitude,2)+POW(d.longitude-c.longitude,2)+POW(d.avg_star-c.avg_star,2) ASC LIMIT 1);
   -- calculate new cluster center
    
    DROP TEMPORARY TABLE IF EXISTS stopping_criteria;
    CREATE TEMPORARY TABLE stopping_criteria select id,latitude,longitude from km_clusters_table2;
    
    
   UPDATE km_clusters_table2 C, (SELECT cluster_id,
       AVG(latitude) AS latitude, AVG(longitude) AS longitude ,AVG(avg_star) AS avg_star
       FROM kmeans_dataset2 GROUP BY cluster_id) D
   SET C.latitude=D.latitude, C.longitude=D.longitude,C.avg_star=D.avg_star WHERE C.id=D.cluster_id;
    
    SET @converge := (select max(abs(t1.latitude-t2.latitude))+max(abs(t1.longitude-t2.longitude)) from stopping_criteria t1, km_clusters_table2 t2 where t1.id=t2.id);
    set @itr_cnt:=@itr_cnt+1;
    
    select @converge,@itr_cnt,@max_itr;

UNTIL @converge <@min_change  OR @itr_cnt>=@max_itr END REPEAT;
DROP TEMPORARY TABLE IF EXISTS stopping_criteria;

IF training = true THEN
    DROP TABLE IF EXISTS km_centroid_train_set;
    CREATE TABLE km_centroid_train_set(
    id int primary key,
    latitude float,
    longitude float,
    avg_star float,
	cluster_predict_error float default 0
    );
    INSERT INTO km_centroid_train_set (id, latitude, longitude,avg_star) SELECT id, latitude, longitude,avg_star FROM km_clusters_table2;
		DROP TABLE IF EXISTS kmeans_final_analysis_data;
       create table kmeans_final_analysis_data (
	user_id char(22) PRIMARY KEY,
	latitude     float,
	longitude  float ,
	avg_star float,   
	cluster_id int DEFAULT 0
  
);
	INSERT INTO kmeans_final_analysis_data(latitude,longitude,user_id,avg_star,cluster_id) SELECT latitude,longitude,user_id,avg_star,cluster_id from kmeans_dataset2;
ELSE
    DROP TABLE IF EXISTS km_centroid_test_set;
    CREATE TABLE km_centroid_test_set(
    id int primary key,
    latitude float,
    longitude float,
    avg_star float,
	cluster_predict_error float default 0
    );
	
    INSERT INTO km_centroid_test_set (id, latitude, longitude,avg_star) SELECT id, latitude, longitude,avg_star FROM km_clusters_table2;
	
	DROP TABLE IF EXISTS kmeans_final_validation_data;
       create table kmeans_final_validation_data (
	user_id char(22) PRIMARY KEY,
	latitude     float,
	longitude  float ,
	avg_star float,   
	cluster_id int DEFAULT 0 
	);
	INSERT INTO kmeans_final_validation_data(latitude,longitude,user_id,avg_star,cluster_id) SELECT latitude,longitude,user_id,avg_star,cluster_id from kmeans_dataset2;
END iF;

COMMIT;
END //
DELIMITER ;