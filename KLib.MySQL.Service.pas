{
  KLib Version = 3.0
  The Clear BSD License

  Copyright (c) 2020 by Karol De Nery Ortiz LLave. All rights reserved.
  zitrokarol@gmail.com

  Redistribution and use in source and binary forms, with or without
  modification, are permitted (subject to the limitations in the disclaimer
  below) provided that the following conditions are met:

  * Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

  * Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

  * Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.

  NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY
  THIS LICENSE. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
  CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
  IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
  POSSIBILITY OF SUCH DAMAGE.
}

unit KLib.MySQL.Service;

interface

uses

  KLib.MySQL.Credentials, KLib.MySQL.Info,
  KLib.Types,
  Winapi.Windows,
  System.Classes;

type

  TMySQLService = class
  private
    _nameService: string;
    function getMysqlCredentials: KLib.MySQL.Credentials.TCredentials;
    procedure setMysqlCredentials(value: KLib.MySQL.Credentials.TCredentials);
    function getPort: integer;
    procedure setPort(value: integer);
  public
    info: TMySQLInfo;
    property nameService: string read _nameService;
    property mysqlCredentials: KLib.MySQL.Credentials.TCredentials read getMysqlCredentials write setMysqlCredentials;
    property port: integer read getPort write setPort;

    constructor create(nameService: string; mySQLInfo: TMySQLInfo); overload;
    constructor create(nameService: string); overload;

    procedure AStart(handleSender: HWND; forceCleanDataDir: boolean = false);
    procedure startIfExists(forceCleanDataDir: boolean = false);
    procedure start(forceCleanDataDir: boolean = false);
    procedure stopIfExists;
    procedure stop;
    function isRunning: boolean;
    function existsService: boolean;
    procedure deleteService;

    procedure ACreateService(myCallbacks: TCallBacks; forceInstall: boolean = false);
    procedure createService(forceInstall: boolean = false);

    procedure cleanDataDir;

    procedure addFirewallException;
    procedure deleteFirewallException;
    procedure mysqldump(databasesList: TStringList; fileNameOut: string; skipTriggers: boolean);
    procedure mysqlpump(databasesList: TStringList; fileNameOut: string; skipTriggers: boolean);
    procedure importScript(fileNameIn: string);
  end;

implementation

uses
  KLib.MySQL.CLIUtilities, KLib.MySQL.Utils,
  KLib.WindowsService, KLib.Windows, KLib.Constants, KLib.Utils, KLib.Validate,
  Klib.Asyncify,
  System.SysUtils;

constructor TMySQLService.Create(nameService: string; mySQLInfo: TMySQLInfo);
begin
  create(nameService);
  validateThatFileExists(mySQLInfo.path_ini);
  validateThatFileExists(mySQLInfo.path_mysqld);
  Self.info := mySQLInfo;
end;

constructor TMySQLService.create(nameService: string);
const
  ERR_MSG = 'MySQL service name were not being specified.';
begin
  validateThatStringIsNotEmpty(nameService, ERR_MSG);
  Self._nameService := nameService;
end;

procedure TMySQLService.AStart(handleSender: HWND; forceCleanDataDir: boolean = false);
begin
  if forceCleanDataDir then
  begin
    cleanDataDir;
  end;
  TWindowsService.aStart(handleSender, nameService);
end;

procedure TMySQLService.startIfExists(forceCleanDataDir: boolean = false);
begin
  if forceCleanDataDir then
  begin
    cleanDataDir;
  end;
  TWindowsService.startIfExists(nameService);
end;

procedure TMySQLService.start(forceCleanDataDir: boolean = false);
begin
  if forceCleanDataDir then
  begin
    cleanDataDir;
  end;
  TWindowsService.start(nameService);
end;

procedure TMySQLService.stopIfExists;
begin
  TWindowsService.stopIfExists(nameService);
end;

procedure TMySQLService.Stop;
begin
  TWindowsService.stop(nameService);
end;

function TMySQLService.isRunning: boolean;
begin
  Result := TWindowsService.checkIfIsRunning(nameService);
end;

function TMySQLService.existsService: boolean;
begin
  Result := TWindowsService.checkIfExists(nameService);
end;

procedure TMySQLService.deleteService;
begin
  TWindowsService.delete(nameService);
end;

procedure TMySQLService.ACreateService(myCallbacks: TCallBacks; forceInstall: boolean = false);
begin
  asyncify(
    procedure
    begin
      createService(forceInstall);
    end,
    myCallbacks);
end;

procedure TMySQLService.createService(forceInstall: boolean = false);
const
  ERR_MSG_INI_FILE_NOT_SPECIFIED = 'MySQL Ini file were not being spefified.';
  ERR_MSG_SERVICE_ALREADY_EXISTS = 'A service with the same name already exists.';
  ERR_MSG_SERVICE_NOT_CREATED = 'MySQL Service not created.';

  RAISE_EXCEPTION_IF_FUNCTION_FAILS = true;
var
  _alreadyExistsService: boolean;
  _doubleQuotedIniPath: string;
  _mysqldParamsCreateService: string;
  _cmdParams: string;
begin
  if info.path_ini = '' then
  begin
    raise Exception.Create(ERR_MSG_INI_FILE_NOT_SPECIFIED);
  end;
  _alreadyExistsService := existsService;
  if _alreadyExistsService then
  begin
    if forceInstall then
    begin
      deleteService;
    end
    else
    begin
      raise Exception.Create(ERR_MSG_SERVICE_ALREADY_EXISTS);
    end;
  end;
  _doubleQuotedIniPath := getDoubleQuotedString(info.path_ini);
  _mysqldParamsCreateService := '--install ' + nameService + ' --defaults-file=' + _doubleQuotedIniPath;
  _cmdParams := '/K "' + getDoubleQuotedString(info.path_mysqld) + ' ' + _mysqldParamsCreateService + '"' + ' & EXIT';
  shellExecuteExCMDAndWait(_cmdParams, RUN_AS_ADMIN, TShowWindowType._SW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);
  if not(existsService) then
  begin
    raise Exception.Create(ERR_MSG_SERVICE_NOT_CREATED);
  end;
end;

procedure TMySQLService.cleanDataDir;
begin
  case info.version of
    v5_5:
      //todo
      ;
    v5_7:
      cleanDataDir_v5_7(info.path_datadirIniFile);
    v_8:
      cleanDataDir_v8(info.path_datadirIniFile);
  end;
end;

procedure TMySQLService.addFirewallException;
const
  DESCRIPTION_SERVICE_MYSQL = 'Database MySQL';
  GROUP_SERVICE_MYSQL = 'MySQL';
begin
  addTCP_IN_FirewallException(nameService, port, DESCRIPTION_SERVICE_MYSQL, GROUP_SERVICE_MYSQL);
end;

procedure TMySQLService.deleteFirewallException;
begin
  KLib.Windows.deleteFirewallException(nameService);
end;

procedure TMySQLService.mysqldump(databasesList: TStringList; fileNameOut: string; skipTriggers: boolean);
const
  NOT_DUMP_ALL_NON_STANDARD_DATABASES = false;
var
  _argsMysqldump: TMysqldumpArgs;
begin
  _argsMysqldump.fileNameOut := fileNameOut;
  _argsMysqldump.databasesList := databasesList;
  _argsMysqldump.skipTriggers := skipTriggers;
  with _argsMysqldump do
  begin
    pathMysqldumpExe := info.path_mysqldump;
    credentials := self.mysqlCredentials;
    dumpAllNonStandardDatabases := NOT_DUMP_ALL_NON_STANDARD_DATABASES;
  end;
  KLib.MySQL.CLIUtilities.mysqldump(_argsMysqldump);
end;

procedure TMySQLService.mysqlpump(databasesList: TStringList; fileNameOut: string; skipTriggers: boolean);
const
  NOT_DUMP_ALL_NON_STANDARD_DATABASES = false;
var
  _argsMysqldump: TMysqldumpArgs;
begin
  _argsMysqldump.fileNameOut := fileNameOut;
  _argsMysqldump.databasesList := databasesList;
  _argsMysqldump.skipTriggers := skipTriggers;
  with _argsMysqldump do
  begin
    pathMysqldumpExe := info.path_mysqlpump;
    credentials := self.mysqlCredentials;
    dumpAllNonStandardDatabases := NOT_DUMP_ALL_NON_STANDARD_DATABASES;
  end;
  KLib.MySQL.CLIUtilities.mysqldump(_argsMysqldump);
end;

procedure TMySQLService.importScript(fileNameIn: string);
var
  _pathMysqlCli: string;
begin
  _pathMysqlCli := info.path_mysql;
  KLib.MySQL.CLIUtilities.importScript(_pathMysqlCli, fileNameIn, mysqlCredentials);
end;

function TMySQLService.getMysqlCredentials: KLib.MySQL.Credentials.TCredentials;
begin
  Result := info.credentials;
end;

procedure TMySQLService.setMysqlCredentials(value: KLib.MySQL.Credentials.TCredentials);
var
  _tempCredentials: KLib.MySQL.Credentials.TCredentials;
begin
  _tempCredentials := value;
  with _tempCredentials do
  begin
    server := LOCALHOST_IP_ADDRESS;
    database := '';
  end;
  info.credentials := value;
end;

function TMySQLService.getPort: integer;
begin
  Result := info.portIniFile;
end;

procedure TMySQLService.setPort(value: integer);
begin
  info.portIniFile := value;
end;

end.
