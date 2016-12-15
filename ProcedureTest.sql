use GlobalDB
go


alter  procedure [dbo].[PROC_CombineUsersWorkedTogether]  @pResult int OUTPUT
AS
BEGIN TRY
	BEGIN TRANSACTION		
		DECLARE @lRepoNo bigint;
		DECLARE @lRepoCreate nvarchar(65);
		DECLARE @lLogin nvarchar(80);
		DECLARE @lRepoNoCur bigint;
		DECLARE @lRepoCreateCur nvarchar(65);
		DECLARE @lLoginCur nvarchar(80);
		
		DECLARE @lUsers nvarchar(max);
				
		DECLARE cur CURSOR FOR SELECT RepoNo, ActivityCreateYYYYMM, UserName FROM GlobalDB.dbo.Step3_Filter_WorkedUsrs_Unq1 order by RepoNo, ActivityCreateYYYYMM, UserName;
			
		set @pResult =-1;
		OPEN cur;
		FETCH NEXT FROM cur into @lRepoNoCur, @lRepoCreateCur, @lLoginCur;
		SET @lRepoNo = LTRIM(RTRIM(@lRepoNoCur));
		SET @lRepoCreate = LTRIM(RTRIM(@lRepoCreateCur));		
		
		SET @lRepoNoCur = LTRIM(RTRIM(@lRepoNoCur));
		SET @lRepoCreateCur = LTRIM(RTRIM(@lRepoCreateCur));
		SET @lLoginCur = LTRIM(RTRIM(@lLoginCur));		
		SET @lUsers = @lLoginCur;

		FETCH NEXT FROM cur into @lRepoNoCur, @lRepoCreateCur, @lLoginCur;
		SET @lRepoNoCur = LTRIM(RTRIM(@lRepoNoCur));
		SET @lRepoCreateCur = LTRIM(RTRIM(@lRepoCreateCur));
		SET @lLoginCur = LTRIM(RTRIM(@lLoginCur));		
					
		WHILE @@FETCH_STATUS=0
		BEGIN
			WHILE  @lRepoNo = @lRepoNoCur and @lRepoCreate = @lRepoCreateCur			
			BEGIN
				SET @lUsers += ',' + @lLoginCur;

				FETCH NEXT FROM cur into @lRepoNoCur, @lRepoCreateCur, @lLoginCur;
					SET @lRepoNoCur = LTRIM(RTRIM(@lRepoNoCur));
					SET @lRepoCreateCur = LTRIM(RTRIM(@lRepoCreateCur));
					SET @lLoginCur = LTRIM(RTRIM(@lLoginCur));
				if @@FETCH_STATUS <> 0 
				 break;
			END;
			
			insert into Step3_Filter_WorkedUsrs_Unq_Users (RepoNo, ActivityCreateYYYYMM,Users) values (@lRepoNo, @lRepoCreate,@lUsers );
			SET @lUsers = @lLoginCur;

			SET @lRepoNo = @lRepoNoCur;
		    SET @lRepoCreate = @lRepoCreateCur;	
			FETCH NEXT FROM cur into @lRepoNoCur, @lRepoCreateCur, @lLoginCur;	
			
			SET @lRepoNoCur = LTRIM(RTRIM(@lRepoNoCur));
		    SET @lRepoCreateCur = LTRIM(RTRIM(@lRepoCreateCur));
		    SET @lLoginCur = LTRIM(RTRIM(@lLoginCur));	            
						
		END;
		insert into Step3_Filter_WorkedUsrs_Unq_Users (RepoNo, ActivityCreateYYYYMM,Users) values (@lRepoNo, @lRepoCreate,@lUsers );
		
		set @pResult =0;
		CLOSE cur;
		DEALLOCATE cur;	
	COMMIT; 
	return @pResult;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0
  begin
     ROLLBACK;
     CLOSE cur;
		 DEALLOCATE cur;	
     end;
  declare @ErrMsg nvarchar(4000), @ErrSeverity int, @xstate int, @error int;
  select @ErrMsg= ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY(), @xstate=XACT_STATE(), @error=ERROR_NUMBER();
  RAISERROR (@ErrMsg, @ErrSeverity, 1);
END CATCH;
