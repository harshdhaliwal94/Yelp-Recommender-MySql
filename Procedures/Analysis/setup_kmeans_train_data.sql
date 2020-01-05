DELIMITER //
CREATE PROCEDURE setup_kmeans_train_data
(
)
BEGIN
   DECLARE user_id1 char(22);
   DECLARE latitude1    float;
   DECLARE longitude1  float ;
    DECLARE avg_star1 float;
    DECLARE done int default 0;
    DECLARE train_set int(10) default 0;
    DECLARE cnt_i int(10) default 0;

   DECLARE curs CURSOR  FOR select (100*user_est_lat/max(user_est_lat)over(rows between unbounded preceding and unbounded following)) as lat,(100*user_est_long/max(user_est_long)over(rows between unbounded preceding and unbounded following)) as lng,
    r.user_id,
    (avg_star) as avg_star
    from
    (select (sum(latitude*times)/sum(times)) as user_est_lat,(sum(longitude*times)/sum(times)) as user_est_long,user_id,
(sum(stars*times)/sum(times)) as avg_star    from final_dataset_train group by user_id)r;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    SET @train_set := floor((select count(*) from final_dataset)*0.7);
    SET @cnt_i := 0;
    drop table if exists final_dataset_train;
   create table final_dataset_train (
   business_id char(22) ,
   user_id char(22) ,
   latitude float,
   longitude float,
   stars float,
   times int
   );
   INSERT INTO final_dataset_train (business_id, user_id, latitude, longitude, stars,times) select business_id, user_id, latitude, longitude, stars,times from final_dataset where (@cnt_i:=@cnt_i+1) between 1 and @train_set;

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