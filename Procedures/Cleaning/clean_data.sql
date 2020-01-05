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