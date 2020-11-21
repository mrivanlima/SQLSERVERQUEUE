USE Practice
GO

sp_configure 'show advanced options', 1
GO
RECONFIGURE
GO
sp_configure 'Blocked process threshold', 6
GO
RECONFIGURE
GO




CREATE TABLE MonitorEventLockInformation
(
Id BigInt Identity(1,1),
MessageBody XML,
DatabaseID Int,
Process XML,
Is_Notified Bit Default(0)
)
GO


CREATE OR ALTER PROCEDURE spProductionMonitorServiceProc
AS
BEGIN
	DECLARE @message TABLE ( message_body xml not null,
			message_sequence_number int not null );

	RECEIVE message_body, message_sequence_number
	FROM LockQueue
	INTO @message;
	INSERT INTO MonitorEventLockInformation(MessageBody,DatabaseID,Process)
	SELECT	message_body,
			DatabaseId = CAST( message_body AS XML ).value( '(/EVENT_INSTANCE/DatabaseID)[1]', 'int' ),
			Process = CAST( message_body AS XML ).query( '/EVENT_INSTANCE/TextData/blocked-process-report/blocked-process/process' )
	FROM @message
	ORDER BY message_sequence_number
END
GO

--drop QUEUE LockQueue

CREATE QUEUE LockQueue 
   WITH STATUS = ON , 
   RETENTION = OFF, 
   ACTIVATION ( 
                  STATUS = ON, 
                  PROCEDURE_NAME = spProductionMonitorServiceProc, 
                  MAX_QUEUE_READERS = 10, 
                  EXECUTE AS SELF
			   )

			--   drop service LockService
CREATE SERVICE LockService
ON QUEUE LockQueue ( [http://schemas.microsoft.com/SQL/Notifications/PostEventNotification] )


--drop ROUTE NotifyRoute
--CREATE ROUTE NotifyRoute  
--WITH SERVICE_NAME = 'LockService',  
--ADDRESS = 'LOCAL';

--drop EVENT NOTIFICATION NotifyLocks ON SERVER 
CREATE EVENT NOTIFICATION NotifyLocks
ON SERVER
WITH fan_in
FOR blocked_process_report
TO SERVICE 'LockService', 'current database';


TRUNCATE TABLE MonitorEventLockInformation
SELECT * FROM MonitorEventLockInformation

SELECT *
FROM master.sys.syslogins;

-----------------------------------
/*BEGIN TRANSACTION 
UPDATE  l
SET LockName = 'Row Locking Everthin'
FROM Lock l WITH (TABLOCK)
WHERE id = 1

COMMIT;


SELECT * FROM Lock
*/


SELECT is_broker_enabled FROM sys.databases WHERE name = (SELECT DB_NAME())
GO



CREATE OR ALTER PROCEDURE dbo.spSendEmail
@BlockedCommand VARCHAR(MAX),
@BlockingCommand VARCHAR(MAX)
--WITH EXECUTE AS OWNER
AS

BEGIN
          DECLARE @message VARCHAR(MAX) = 'Command ' + @BlockedCommand + ' is blocking ' + @BlockingCommand;
          EXEC msdb.dbo.sp_send_dbmail @profile_name='TestUser',
          @recipients='junk@misterivanlima.com',--@recipients='junk@misterivanlima.com',
          @subject='Locking alert',
          @body= @message
END
GO



CREATE OR ALTER TRIGGER dbo.TriggerLockingEmailAlert
ON dbo.MonitorEventLockInformation 
AFTER INSERT
AS
BEGIN
    DECLARE @BlockedCommand VARCHAR(MAX);
    DECLARE @BlockingCommand VARCHAR(MAX);

SELECT  @BlockedCommand = MessageBody.value( '(/EVENT_INSTANCE/TextData/blocked-process-report/blocked-process/process)[1]', 'varchar(max)' ),
        @BlockingCommand = MessageBody.value( '(/EVENT_INSTANCE/TextData/blocked-process-report/blocking-process/process)[1]', 'varchar(max)' )
         

FROM inserted

EXEC  dbo.spSendEmail @BlockedCommand = @BlockedCommand, @BlockingCommand = @BlockingCommand

END
go

USE msdb
GRANT EXECUTE ON msdb.dbo.sp_send_dbmail TO [public]
