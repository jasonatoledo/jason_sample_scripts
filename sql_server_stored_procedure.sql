-- create function to return next dvd in stock for a customer
CREATE OR ALTER FUNCTION Fun_InStockDVDId 
(@MemID numeric(12))

-- return value has same length as DVDId max length
RETURNS numeric(16)

AS

BEGIN

	DECLARE @NextDVDId numeric(16);
	-- ensure the memberId is part of the queue
	IF EXISTS (SELECT MemberId FROM RentalQueue WHERE MemberId = @MemID)
																		
		-- set
		SET @NextDVDId = -- select statement to return first DVDId in the queue
						 -- that is available for the member id parameter
							(SELECT	TOP 1 
									RentalQueue.DVDId -- only select the first dvdid in the queue
								FROM DVD
									JOIN RentalQueue ON DVD.DVDId = RentalQueue.DVDId
								WHERE RentalQueue.MemberId = @MemID
									AND DVD.DVDQuantityOnHand >= 1 -- ensure DVD qty is at least 1
								ORDER BY RentalQueue.QueuePosition
							)
	ELSE
		SET @NEXTDVDId = NULL -- if member doesn't have DVDs in queue return NULL

	RETURN @NextDVDId; -- function will return one of the two conditions



-- create function to return # of additional DVDs that can be rented this month
CREATE OR ALTER FUNCTION Fun_AdditionalDVDs
(@MemID numeric(12))
RETURNS numeric(2)  -- return value has max of 2 digits
AS
BEGIN

	-- declare variables for different counts and limits
	DECLARE @NumDVDsAvailable NUMERIC(2); -- this will be used as the return value
	DECLARE @DVDLimitPerMonth NUMERIC(2); -- DVD limit per membership plan
	DECLARE @DvdsRentedThisMonth NUMERIC(2); -- # of dvds member has rented this month
	DECLARE @DVDsRemainingMonth NUMERIC(2); -- remaining dvds for this calendar month
	DECLARE @DVDsOutRightNow NUMERIC(2); -- get number of dvds member has out currently
	DECLARE @DVDsAtATimeValue NUMERIC(2); -- get current dvd limit based on member plan
	-- need to check if the user has rental records out
	IF @MemID IN (SELECT MemberId FROM Rental WHERE Rental.MemberId = @MemID)
	BEGIN

	-- get dvd limit for a member based on membership
	SET @DVDLimitPerMonth = 
		(SELECT  MembershipLimitPerMonth
			FROM Membership
			JOIN Member ON Membership.MembershipId = Member.MembershipId
			WHERE MemberId = @Memid)

	-- get the number of dvds rented this month based on members ship date
	SET @DvdsRentedThisMonth =
		(SELECT COUNT(DVDId) AS DVDRentedThisMonth
			FROM Rental
			WHERE RentalShippedDate IS NOT NULL
			AND MemberId = @Memid
			AND RentalShippedDate > EOMONTH(DATEADD(MONTH, -1, GETDATE()))
			AND RentalShippedDate < EOMONTH(GETDATE()))
	
	-- get the number of remaining DVDs a member can rent this month
	SET @DVDsRemainingMonth = @DVDLimitPerMonth - @DvdsRentedThisMonth

	-- get # of dvds out by the member
	SET @DVDsOutRightNow = (SELECT COUNT(DVDId) AS DVDsOut
								FROM Rental
								WHERE Rental.RentalReturnedDate IS NULL
								AND Rental.MemberId = @MemID
								GROUP BY Rental.MemberId)
	-- get # of dvds a member can have at a time
	SET @DVDsAtATimeValue = (SELECT Membership.DVDAtTime
								FROM Membership
								JOIN Member ON Membership.MembershipId = Member.MembershipId
								WHERE Member.MemberId = @MemID)
	-- calculate return value - if user does not have any rentals, then dvd at at time value
	SET @NumDVDsAvailable = CASE WHEN @DVDsRemainingMonth < (@DVDsAtATimeValue - @DVDsOutRightNow)
								 THEN @DVDsRemainingMonth
								 WHEN (@DVDsAtATimeValue - @DVDsOutRightNow) < @DVDsRemainingMonth
								 THEN (@DVDsAtATimeValue - @DVDsOutRightNow)
								 END
	 END
	 -- if the member does not have any rentals then default to dvd at a time value per plan
	 ELSE IF @MemID NOT IN	(SELECT MemberId FROM Rental WHERE Rental.MemberId = @MemID) 
		 SET @NumDVDsAvailable =  (SELECT Membership.DVDAtTime
									FROM Membership
									JOIN Member ON Membership.MembershipId = Member.MembershipId
									WHERE Member.MemberId = @MemID)
     RETURN @NumDVDsAvailable;
END;


-- stored procedure to process lost DVD or return and determine number of DVDs to be sent
CREATE OR ALTER PROCEDURE SP_ProcessDVDSendDVD
	@P_MemberId NUMERIC     -- This parameter is the MemberId returning or lost a DVD
	, @P_DVDId NUMERIC      -- This parameter is the DVDId that is returned/lost
	, @P_Action NUMERIC     -- This parameter is whether the DVD was lost or returned
							-- 0 = return, 1 = lost DVD
AS

BEGIN
	
	-- create conditional where @P_DVDId has to be rented by the member currently
	IF @P_DVDId NOT IN (SELECT DVDId FROM Rental WHERE MemberId = @P_MemberId AND RentalReturnedDate IS NULL)
	BEGIN
		RAISERROR('This member does not have this DVD out on rental!', 16,1);
		IF @@ERROR <> 0 RETURN -- stop the  stored procedure from continuing if DVD is not valid
	END


	-- start loop based on action selected from the front-end for the user (not manually typed)
	ELSE IF @P_Action = 1 AND @P_DVDId  IN 
			(SELECT DVDId FROM Rental WHERE MemberId = @P_MemberId AND RentalReturnedDate IS NULL)

		BEGIN       -- insert record of lost DVD into LostDVDHistory table and run lost price function
			INSERT INTO LostDVDHistory(LostDVDRecordId,MemberId, DVDId, ChargeForDVD)
			VALUES(NEXT VALUE FOR SEQ_LostDVDId, @P_MemberId, @P_DVDId, dbo.Fun_GetDVDLostPrice(@P_MemberId));

			-- increase DVD table lost quantity value +1 for that DVDId
			UPDATE DVD
			SET DVDLostQuantity = DVDLostQuantity + 1
			WHERE DVDId = @P_DVDId
		END

	ELSE
	-- if the member action selected is a returned dvd
	IF @P_Action = 0 AND @P_DVDId  IN 
			(SELECT DVDId FROM Rental WHERE MemberId = @P_MemberId AND RentalReturnedDate IS NULL)
		BEGIN
		-- declare variables to insert values into
		DECLARE @V_NumDVDsRentable NUMERIC = 0;
		DECLARE @V_NextDVDInStock NUMERIC = 0;
		DECLARE @V_NumDVDsConstant INTEGER = 0;

			-- increase DVD table on hand quantity value +1 for returned DVDId
			UPDATE DVD
			SET DVDQuantityOnHand = DVDQuantityOnHand + 1
			WHERE DVDId = @P_DVDId;
		
			-- update rental return date value in rentals table for the returned DVD
			UPDATE Rental
			SET RentalReturnedDate = GETDATE()
			WHERE DVDId = @P_DVDId AND MemberId = @P_MemberId;

		END	
		--Extra credit deplete available rentals for multiple rentals in queue
		BEGIN
			-- initiate function from #2 to get number of additional DVDs to rent
			SET @V_NumDVDsRentable = (dbo.Fun_AdditionalDVDs(@P_MemberId));
			-- set constant value for SP output statement
			SET @V_NumDVDsConstant = (dbo.Fun_AdditionalDVDs(@P_MemberId));

			-- while statement for loop conditional based on # of dvds rentable
			WHILE (@V_NumDVDsRentable > 0)
			BEGIN
			-- initiate function from #1 to get next movie in stock for member
			SET @V_NextDVDInStock = (dbo.Fun_InStockDVDId(@P_MemberId));

			-- initiate SP from HW #2 to remove the DVD from member's queue
			EXECUTE SP_DeleteDVDRentalQueue @P_MemberId, @V_NextDVDInStock;

			-- create rental record of the next in stock DVD from queue
			INSERT INTO Rental (RentalId, MemberId, DVDId, RentalRequestDate)
			VALUES (NEXT VALUE FOR SEQ_RentalIdCreation, @P_MemberId, @V_NextDVDInStock, GETDATE())

			-- decrease DVD table on hand quantity value -1 for that DVDId
			UPDATE DVD
			SET DVDQuantityOnHand = DVDQuantityOnHand - 1
			WHERE DVDId = @V_NextDVDInStock;

			-- increase DVD table on rent quantity value +1 for that DVDId
			UPDATE DVD
			SET DVDQuantityOnRent = DVDQuantityOnRent + 1
			WHERE DVDId = @V_NextDVDInStock;

			-- decrease rentable DVDs by 1 value 
			SET @V_NumDVDsRentable = @V_NumDVDsRentable - 1 
			END
		-- display dvds that were rented and removed from the member queue as output
		SELECT TOP (@V_NumDVDsConstant)
				DVD.DVDTitle	AS DVDRented	
				, GETDATE() AS RentalRequestDate
		FROM DVD
		JOIN Rental ON DVD.DVDId = Rental.DVDId
		WHERE Rental.MemberId = @P_MemberId;
			-- reduce value of number of rentable DVDs
		END
			
END;
