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
/*DECLARE cur_user CURSOR FOR select user_id from user_in_cluster;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;*/

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
DELIMITER ; //