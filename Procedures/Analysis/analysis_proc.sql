DELIMITER //
CREATE PROCEDURE analysis_proc
(
IN incity varchar(50)
)
BEGIN
	/* Has sequence for data processing for analysis and run analysis on it, store its results*/
	  DECLARE training    boolean;
	   DECLARE k int;
	SET @training:=true;
	SET @k:=20;
	CALL setup_data(incity);
	CALL setup_kmeans_train_data();
	CALL kmeans2(@k,@training);    
END; //
DELIMITER ;