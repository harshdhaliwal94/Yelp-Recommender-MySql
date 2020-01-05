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