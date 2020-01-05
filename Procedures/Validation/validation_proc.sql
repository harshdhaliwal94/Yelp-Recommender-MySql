DELIMITER //
CREATE PROCEDURE validation_proc
(
)
BEGIN
	/* Has sequence for data processing for validation and run validation on it, store its results*/
	  DECLARE training    boolean;
	   DECLARE k int;
	SET @training:=false;
	SET @k:=20;
	CALL setup_kmeans_data2();
	CALL kmeans2(@k,@training);
    CALL validate_prediction(@k);
END; //
DELIMITER ;