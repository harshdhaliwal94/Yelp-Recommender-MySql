/*Creating required tables*/

create table if not exists business (

business_id char(22),
name varchar(100) ,
address varchar(150),
city  varchar(50),
state  varchar(30) ,
postal_code varchar(20),
latitude     float,
longitude  float ,
stars  float,
review_count int(5),
is_open       int(1),
attributes JSON,
Categories text,
hours JSON
primary key(business_id)   
);

#alter table business add primary key(business_id);
#create index latitude on business(latitude);
#create index longitude on business(longitude);
#create index city on business(city);
 
create table if not exists review (
review_id  char(22),
user_id char(22),
business_id char(22),
stars float,
date date ,
useful int(5),
funny int(5),
cool   int(5)
primary key(review_id)       
);

#alter table review add primary key(review_id);
#create index user_id on review(user_id);
#create index stars on review(stars);
#create index date_review on review(date);
#create index b_u_ind on review(business_id,user_id);
#create index business_id on review(business_id);
   
   
create table if not exists user (
user_id char(22) not null,
name varchar(100) not null,
review_count int(5) default 0,
yelping_since datetime,
useful int(5),
funny int(5),
cool int(5) ,
elite varchar(150),
fans int(5),
average_stars float,
compliment_hot int(5),
compliment_more int(5),
compliment_profile int(5),
compliment_cute int(5),
compliment_list int(5),
compliment_note int(5),
compliment_plain int(5),
compliment_cool int(5),
compliment_funny int(5),
compliment_writer int(5),
compliment_photos int(5)
primary key(user_id)
);

#alter table user add primary key(user_id);
#create index review_count on user(review_count);
#create index yelping_since on user(yelping_since);
   

/*Stored Prcedures for cleaning data, creating training data, and k-means clustering*/

DROP PROCEDURE IF EXISTS clean_data;
DELIMITER //
CREATE PROCEDURE clean_data
(
)
BEGIN
	DECLARE cnt1 INT DEFAULT 0;
	 DECLARE done int default 0;
	 	DECLARE business_id1 char(22);
    DECLARE user_id1 char(22);
	
	DECLARE curs1 CURSOR  FOR select c.b_id from (select distinct(business_id) as b_id from review where business_id not  in (select distinct(business_id)from business) )as c;
	DECLARE curs2 CURSOR  FOR select c.u_id from (select distinct(user_id) as u_id from review where user_id not  in (select distinct(user_id)from user) )as c;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

	/*Cleaning for business table*/
    delete from business where address='' or latitude='' or longitude='' or city='' or business_id='';
	/*Cleaning for reviews table*/
    delete from review where user_id='' or business_id='' or review_id='';
	/*Commented to save time since it takes approx 25 mins to clean
	 OPEN curs1 ;
   cleanreview: LOOP
      FETCH curs1 INTO business_id1;
		SET cnt1=cnt1+1;
		select cnt1;
		  IF done = 1 THEN
            LEAVE cleanreview;
        END IF;
		delete from review where business_id=business_id1;
		 
	END LOOP;
      
	CLOSE curs1;
	SET @cnt1=0;
	SET @done=0;
	 OPEN curs2 ;
   cleanreview1: LOOP
      FETCH curs2 INTO user_id1;
		SET cnt1=cnt1+1;
		select cnt1;
		  IF done = 1 THEN
            LEAVE cleanreview1;
        END IF;
		
		delete from review where user_id=user_id1;
		 
	END LOOP;
      
	CLOSE curs2;
	*/
	/*Cleaning for users table*/
	delete from user where user_id='';
	
COMMIT;  
END; //
DELIMITER ;


DROP PROCEDURE IF EXISTS analysis_proc;
DELIMITER //
CREATE PROCEDURE analysis_proc
(
IN incity varchar(50),
IN k_param int
)
BEGIN
	/* Has sequence for data processing for analysis and run analysis on it, store its results*/
	  DECLARE training    boolean;
	   DECLARE k int;
	SET @training:=true;
	SET @k:=k_param;
	/*CALL setup_data(incity);*/
	CALL setup_kmeans_train_data();
	CALL kmeans2(@k,@training);    
END; //
DELIMITER ;


DROP PROCEDURE IF EXISTS validation_proc;
DELIMITER //
CREATE PROCEDURE validation_proc
(
	IN k_param int
)
BEGIN
	/* Has sequence for data processing for validation and run validation on it, store its results*/
	  DECLARE training    boolean;
	   DECLARE k int;
	SET @training:=false;
	SET @k:=k_param;
	CALL setup_kmeans_data2();
	CALL kmeans2(@k,@training);
    CALL validate_prediction(@k);
END; //
DELIMITER ;


DROP PROCEDURE IF EXISTS setup_data;
DELIMITER //
CREATE PROCEDURE setup_data
(
IN incity varchar(50)
)
BEGIN
    DECLARE cnt1 INT DEFAULT 0;
	DECLARE business_id1 char(22);
    DECLARE user_id1 char(22);
    DECLARE latitude1    float;
    DECLARE longitude1  float ;   
    DECLARE stars1 float;
    DECLARE times1 int;
	 DECLARE done int default 0;
    DECLARE curs CURSOR  FOR select r.business_id,r.user_id,b.latitude,b.longitude,r.sta as avgstars,r.cnt as times from (select business_id,user_id,count(*)cnt,avg(stars) as sta from review where  date>='2018-09-01' group by business_id,user_id )r,(select user_id from user where review_count>200) u,(select business_id,latitude,longitude from business where city=incity) b where r.user_id=u.user_id   and r.business_id=b.business_id    ;
   DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
   select 'This is data processing for analysis it will 6min54sec please wait ' as msg;
   DROP TABLE IF EXISTS final_dataset ;
    create table final_dataset (
business_id char(22),
user_id char(22),
latitude     float,
longitude  float ,   
stars float,
times int,
cluster_id int DEFAULT 0
  
);
   OPEN curs ;
   checkdata: LOOP
      FETCH curs INTO business_id1,user_id1,latitude1,longitude1,stars1,times1;
		SET cnt1=cnt1+1;
		select cnt1;
		  IF done = 1 THEN
            LEAVE checkdata;
        END IF;
		insert into final_dataset(business_id,user_id,latitude,longitude,stars,times) values(business_id1,user_id1,latitude1,longitude1,stars1,times1);
		 
	END LOOP;
      
	CLOSE curs;
  
COMMIT;   
END; //
DELIMITER ;


DROP PROCEDURE IF EXISTS setup_kmeans_train_data;
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


DROP PROCEDURE IF EXISTS setup_kmeans_data2;
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


DROP PROCEDURE IF EXISTS kmeans2;
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


DROP PROCEDURE IF EXISTS validate_prediction;
DELIMITER //
CREATE PROCEDURE validate_prediction
(
	IN num_clusters int(5)
)
BEGIN
DECLARE cluster_i int(5) default 1;
DECLARE done int default 0;
DECLARE userID char(22);
DECLARE var_userID char(22);
DECLARE cluster_predict_error float;

SET @cluster_i=1;
REPEAT
	DROP TEMPORARY TABLE IF EXISTS user_in_cluster;
	DROP TEMPORARY TABLE IF EXISTS cluster_data;
	DROP TEMPORARY TABLE IF EXISTS cluster_data_copy;
	CREATE TEMPORARY TABLE user_in_cluster
		select distinct user_id from kmeans_dataset2 where cluster_id=@cluster_i;
	CREATE TEMPORARY TABLE cluster_data
		select t1.user_id, t1.business_id, t1.stars, t2.avg_star_cluster, t1.cluster_id 
		from 
			(select k.user_id user_id, f.business_id business_id, f.stars stars, k.cluster_id cluster_id 
				from kmeans_dataset2 k
				inner join final_dataset f on k.user_id=f.user_id order by k.cluster_id) t1, 
			(select avg(f1.stars) as avg_star_cluster, k1.cluster_id as cluster_id 
				from kmeans_dataset2 k1
				inner join final_dataset f1 on k1.user_id=f1.user_id 
				group by k1.cluster_id order by k1.cluster_id) t2
		where t1.cluster_id=t2.cluster_id and t1.cluster_id=@cluster_i;
	CREATE TEMPORARY TABLE cluster_data_copy select * from cluster_data;
	SET @cluster_predict_error:=0;

	BLOCK1: BEGIN
        DECLARE cur_user CURSOR FOR select user_id from user_in_cluster;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; 
        OPEN cur_user;
        cur_user_loop: LOOP
        	FETCH FROM cur_user INTO userID;
        	IF done = 1 THEN
        		CLOSE cur_user;
            	LEAVE cur_user_loop;
        	END IF;

        	SET @var_userID:= (select userID);

        	SET @cluster_predict_error:=@cluster_predict_error +
			(select avg(q1.predict_error) as user_predict_error 
			from
			(
				select t1.business_id, abs(t1.stars - ((IFNULL(avg(t2.stars),avg(t1.avg_star_cluster))+avg(t1.avg_star_cluster))/2)) as predict_error
				from
				(select business_id, stars, avg_star_cluster from cluster_data where user_id=@var_userID) t1 
				left outer join
				(select business_id, stars from cluster_data_copy where user_id<>@var_userID) t2  
				on t1.business_id=t2.business_id
				group by t1.business_id,t1.stars
			)q1);
			select @cluster_i,@var_userID;

        END LOOP cur_user_loop;
        SET done = 0;
    END BLOCK1;
	
	SET @cluster_predict_error:=@cluster_predict_error/(select count(*) from user_in_cluster);
	UPDATE km_centroid_test_set
	SET cluster_predict_error=@cluster_predict_error
	WHERE id=@cluster_i;
	
	select @cluster_predict_error,@cluster_i,num_clusters;
	SET @cluster_i=@cluster_i+1;
UNTIL @cluster_i>num_clusters END REPEAT;
DROP TEMPORARY TABLE IF EXISTS user_in_cluster;
DROP TEMPORARY TABLE IF EXISTS cluster_data;
DROP TEMPORARY TABLE IF EXISTS cluster_data_copy;
COMMIT;
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS sample_recommend;
DELIMITER //
CREATE PROCEDURE sample_recommend
(
IN u_id char(22)
)
BEGIN
	DECLARE c_id INT DEFAULT 0;
	SET @c_id:=(select cluster_id from kmeans_final_validation_data where user_id=u_id);
	select name,CONCAT(address,',',city) location from business,
(select distinct(d1.business_id) b_id ,d1.stars,d1.times   from final_dataset d1  where user_id in (select user_id from kmeans_final_validation_data where cluster_id=@c_id) and business_id NOT IN (select distinct(business_id) from final_dataset where user_id=u_id) and not exists (select * from final_dataset d2 where d2.stars>d1.stars and d2.times>d1.times)
LIMIT 5)rec
where business_id =rec.b_id;
   

COMMIT;        
END; //
DELIMITER ;