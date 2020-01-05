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