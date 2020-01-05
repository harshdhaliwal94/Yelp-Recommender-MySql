DELIMITER //
CREATE PROCEDURE setup_kmeans_data2
(
)
BEGIN
    DECLARE user_id1 char(22);
    DECLARE latitude1    float;
    DECLARE longitude1  float ;
	DECLARE avg_star1 float;
	 DECLARE done int default 0;
    DECLARE curs CURSOR  FOR select (100*user_est_lat/max(user_est_lat)over(rows between unbounded preceding and unbounded following)) as lat,(100*user_est_long/max(user_est_long)over(rows between unbounded preceding and unbounded following)) as lng,
	r.user_id,
	(avg_star) as avg_star
	from
	(select (sum(latitude*times)/sum(times)) as user_est_lat,(sum(longitude*times)/sum(times)) as user_est_long,user_id,
(sum(stars*times)/sum(times)) as avg_star	from final_dataset group by user_id)r;
	 	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	DROP TABLE IF EXISTS kmeans_dataset2;
       create table kmeans_dataset2 (
user_id char(22) PRIMARY KEY,
latitude     float,
longitude  float ,
avg_star float,   
cluster_id int DEFAULT 0
  
);
     OPEN curs ;
   checkdata: LOOP
      FETCH curs INTO latitude1,longitude1,user_id1,avg_star1;
	   IF done = 1 THEN
            LEAVE checkdata;
        END IF;
		insert into kmeans_dataset2(latitude,longitude,user_id,avg_star) values(latitude1,longitude1,user_id1,avg_star1);
		 
	END LOOP;
      
	CLOSE curs;
	
      
  


COMMIT;   
END; //
DELIMITER ;