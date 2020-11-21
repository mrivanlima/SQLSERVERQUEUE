-------------------------------------------------------------------------------
-- Name             : Notifications.sql
-- Date             : 11/09/2020
-- Author           : Ivan Lima (contact@misterivanlima.com)
-- Company          : misterivanlima.com
-- Purpose          : Get alerts from SQL Server to the email
-- Usage            : This should be used in development and QA enviroments. Any grants should not be replicated in prod enviroments.
-- Impact           : If done in Dev and QA, no big impacts to be considered
-------------------------------------------------------------------------------

:setvar TargetDatabase "Practice"
:setvar msdbDatabase "msdb"
:setvar ProfileName "TestUser"
:setvar email "contact@misterivanlima.com"
:setvar EmailSubject "Subject"
:setvar MailUser "[public]"
:setvar dbOwner "sa"

SELECT  '$(profilename)'

USE $(TargetDatabase)
GO

sp_configure 'show advanced options', 1
GO
RECONFIGURE
GO
sp_configure 'Blocked process threshold', 6
GO
RECONFIGURE
GO

DECLARE @IsBrokerEnabled BIT = (SELECT is_broker_enabled FROM sys.databases WHERE name = (SELECT DB_NAME()))

IF @IsBrokerEnabled <> 1
BEGIN
	ALTER DATABASE $(TargetDatabase)  SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE
END
GO

EXEC sp_changedbowner $(dbOwner)
GO

--Optional! For this type of data I would like to have a different schema for manageability purposes.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Utility') 
BEGIN
   EXEC('CREATE SCHEMA Utility')
END
GO

--I will start creating tables to manage alerts starting with Locks.
IF NOT EXISTS (SELECT 1
               FROM sys.tables 
               WHERE name = 'MonitorEventLockInformation' 
			   AND schema_name(schema_id) = 'Utility')
BEGIN
	CREATE TABLE Utility.MonitorEventLockInformation
	(
		Id BigInt Identity(1,1),
		MessageBody XML,
		DatabaseID Int,
		Process XML
	)
END
GO

--This sp is needed before we can create the QUEUE. This will receive information from the QUEUE


CREATE OR ALTER PROCEDURE Utility.spProductionMonitorService
AS
BEGIN
	DECLARE @message TABLE ( message_body xml not null,
			message_sequence_number int not null );

	RECEIVE message_body, message_sequence_number
	FROM LockQueue
	INTO @message;
	INSERT INTO Utility.MonitorEventLockInformation(MessageBody,DatabaseID,Process)
	SELECT	message_body,
			DatabaseId = CAST( message_body AS XML ).value( '(/EVENT_INSTANCE/DatabaseID)[1]', 'int' ),
			Process = CAST( message_body AS XML ).query( '/EVENT_INSTANCE/TextData/blocked-process-report/blocked-process/process' )
	FROM @message
	ORDER BY message_sequence_number
END
GO


IF EXISTS (SELECT 1 
           FROM sys.services 
		   WHERE name = 'LockService')
BEGIN
   DROP SERVICE LockService
END
GO



IF EXISTS (SELECT 1 FROM sys.service_queues WHERE name = 'LockQueue')
BEGIN
   DROP QUEUE LockQueue
END

CREATE QUEUE LockQueue 
   WITH STATUS = ON , 
   RETENTION = OFF, 
   ACTIVATION ( 
                  STATUS = ON, 
                  PROCEDURE_NAME = Utility.spProductionMonitorService, 
                  MAX_QUEUE_READERS = 10, 
                  EXECUTE AS SELF
			   )
GO


CREATE SERVICE LockService
ON QUEUE LockQueue ( [http://schemas.microsoft.com/SQL/Notifications/PostEventNotification] )
GO

--drop ROUTE NotifyRoute
--CREATE ROUTE NotifyRoute  
--WITH SERVICE_NAME = 'LockService',  
--ADDRESS = 'LOCAL';


IF EXISTS (SELECT 1 FROM sys.server_event_notifications)
BEGIN
	DROP EVENT NOTIFICATION NotifyLocks ON SERVER
END
GO


CREATE EVENT NOTIFICATION NotifyLocks
ON SERVER
WITH fan_in
FOR blocked_process_report
TO SERVICE 'LockService', 'current database';

GO

CREATE OR ALTER PROCEDURE utility.spSendEmail
@BlockedCommand VARCHAR(MAX),
@BlockingCommand VARCHAR(MAX)
AS

BEGIN
          DECLARE @message VARCHAR(MAX) = 'Command ' + @BlockedCommand + ' is blocking ' + @BlockingCommand;
          EXEC msdb.dbo.sp_send_dbmail 
		  @profile_name = '$(profilename)',
          @recipients = '$(email)',
          @subject = '$(EmailSubject)',
          @body= @message
END
GO



CREATE OR ALTER TRIGGER Utility.TriggerLockingEmailAlert
ON utility.MonitorEventLockInformation 
AFTER INSERT
AS
BEGIN
    DECLARE @BlockedCommand VARCHAR(MAX);
    DECLARE @BlockingCommand VARCHAR(MAX);

SELECT  @BlockedCommand = MessageBody.value( '(/EVENT_INSTANCE/TextData/blocked-process-report/blocked-process/process)[1]', 'varchar(max)' ),
        @BlockingCommand = MessageBody.value( '(/EVENT_INSTANCE/TextData/blocked-process-report/blocking-process/process)[1]', 'varchar(max)' )
         

FROM inserted

EXEC  utility.spSendEmail @BlockedCommand = @BlockedCommand, @BlockingCommand = @BlockingCommand

END
GO

USE $(msdbDatabase)
GO

GRANT EXECUTE ON msdb.dbo.sp_send_dbmail TO $(MailUser)
GO

USE $(TargetDatabase)
GO

