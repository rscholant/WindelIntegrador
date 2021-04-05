@ECHO OFF

SET SERVICENAME="Windel Integrador"
SET NSSM="%~dp0\nssm.exe"

ECHO INSTALLING SERVICE %SERVICENAME%

%NSSM% stop %SERVICENAME%
%NSSM% remove %SERVICENAME% confirm
%NSSM% install %SERVICENAME% %SERVICENAME%
%NSSM% set %SERVICENAME% Application %~dp0Integrador.exe
%NSSM% set %SERVICENAME% AppDirectory %~dp0
%NSSM% set %SERVICENAME% Description "Integrador Windel"
%NSSM% set %SERVICENAME% Start SERVICE_AUTO_START
%NSSM% set %SERVICENAME% AppStopMethodSkip 0
%NSSM% set %SERVICENAME% AppStopMethodConsole 0
%NSSM% set %SERVICENAME% AppStopMethodWindow 0
%NSSM% set %SERVICENAME% AppStopMethodThreads 0
%NSSM% set %SERVICENAME% AppThrottle 0
%NSSM% set %SERVICENAME% AppExit Default Restart
%NSSM% set %SERVICENAME% AppRestartDelay 500
%NSSM% set %SERVICENAME% AppStdout %~dp0\logs\%SERVICENAME%.log
%NSSM% set %SERVICENAME% AppStderr %~dp0\logs\%SERVICENAME%.log
%NSSM% set %SERVICENAME% AppStdoutCreationDisposition 4
%NSSM% set %SERVICENAME% AppStderrCreationDisposition 4
%NSSM% set %SERVICENAME% AppRotateFiles 1
%NSSM% set %SERVICENAME% AppRotateOnline 0
%NSSM% set %SERVICENAME% AppRotateSeconds 3600
%NSSM% set %SERVICENAME% AppRotateBytes 524288
%NSSM% start %SERVICENAME%