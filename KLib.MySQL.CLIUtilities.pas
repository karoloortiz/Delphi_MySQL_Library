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

unit KLib.MySQL.CLIUtilities;

interface

uses
  KLib.MySQL.Credentials,
  KLib.Constants,
  System.Classes;

type
  TMysqldumpArgs = record
  public
    pathMysqldumpExe: string;
    credentials: TCredentials;
    fileNameOut: string;
    skipTriggers: boolean;
    routines: boolean;

    remove_NO_AUTO_CREATE_USER: boolean;

    dumpAllNonStandardDatabases: boolean;
    databasesList: TStringList;

    procedure clear;
  end;

procedure mysqldump(args: TMysqldumpArgs);
procedure importScript(pathMysqlCli: string; fileNameIn: string; credentials: TCredentials); overload;
procedure importScript(pathMysqlCli: string; fileNameIn: string; connectionString: string); overload;
procedure mysql_upgrade(pathMysql_upgrade: string; credentials: TCredentials; force: boolean = FALSE); overload;
procedure mysql_upgrade(pathMysql_upgrade: string; connectionString: string; force: boolean = FALSE); overload;
procedure mysqladminShutdown(pathMysqladmin: string; credentials: TCredentials;
  force: boolean = NOT_FORCE); overload;
procedure mysqladminShutdown(pathMysqladmin: string; connectionString: string;
  force: boolean = NOT_FORCE); overload;

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.Validate,
  KLib.Windows, KLib.Validate, KLib.Utils,
  System.SysUtils;

const
  SHOW_WINDOW_HIDE = _SW_HIDE;
  RAISE_EXCEPTION_IF_FUNCTION_FAILS = true;

procedure mysqldump(args: TMysqldumpArgs);
var
  _paramsMysqldump: string;
  _databasesList: TStringList;
  _databases: string;
  _cmdParams: string;
begin
  validateThatFileExists(args.pathMysqldumpExe);
  validateMySQLCredentials(args.credentials);
  if args.dumpAllNonStandardDatabases then
  begin
    _databasesList := getNonStandardsDatabasesAsStringList(args.credentials);
  end
  else
  begin
    _databasesList := TStringList.Create;
    _databasesList.Assign(args.databasesList);
  end;

  try
    _databasesList.LineBreak := ' ';
    _databases := _databasesList.Text;

    with args do
    begin
      _paramsMysqldump := credentials.getMySQLCliCredentialsParams;
      _paramsMysqldump := _paramsMysqldump + ' --databases ' + _databases;
      if skipTriggers then
      begin
        _paramsMysqldump := _paramsMysqldump + ' --skip-triggers ';
      end;
      if routines then
      begin
        _paramsMysqldump := _paramsMysqldump + ' --routines ';
      end;
      _paramsMysqldump := _paramsMysqldump + '> ' + getDoubleQuotedString(fileNameOut);
      _cmdParams := '/K "' + getDoubleQuotedString(pathMysqldumpExe) + ' ' + _paramsMysqldump + '"' + ' & EXIT';
    end;
    shellExecuteExCMDAndWait(_cmdParams, RUN_AS_ADMIN, SHOW_WINDOW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);

    if args.remove_NO_AUTO_CREATE_USER then
    begin
      remove_NO_AUTO_CREATE_USER_inDumpFile(args.fileNameOut);
    end;
  finally
    FreeAndNil(_databasesList);
  end;
end;

procedure importScript(pathMysqlCli: string; fileNameIn: string; credentials: TCredentials);
var
  _paramsMysqlCli: string;
  _cmdParams: string;
begin
  validateThatFileExists(fileNameIn);
  validateThatFileExists(pathMysqlCli);
  validateMySQLCredentials(credentials);
  _paramsMysqlCli := credentials.getMySQLCliCredentialsParams;
  _paramsMysqlCli := _paramsMysqlCli + ' < ' + getDoubleQuotedString(fileNameIn);
  _cmdParams := '/K "' + getDoubleQuotedString(pathMysqlCli) + ' ' + _paramsMysqlCli + '"' + ' & EXIT';
  shellExecuteExCMDAndWait(_cmdParams, RUN_AS_ADMIN, SHOW_WINDOW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);
end;

procedure mysql_upgrade(pathMysql_upgrade: string; credentials: TCredentials; force: boolean = FALSE);
var
  _paramsMysql_upgrade: string;
  _cmdParams: string;
begin
  validateThatFileExists(pathMysql_upgrade);
  validateMySQLCredentials(credentials);
  _paramsMysql_upgrade := credentials.getMySQLCliCredentialsParams;
  if force then
  begin
    _paramsMysql_upgrade := _paramsMysql_upgrade + ' --force';
  end;
  _cmdParams := '/K "' + getDoubleQuotedString(pathMysql_upgrade) + ' ' + _paramsMysql_upgrade + '"' + ' & EXIT';
  shellExecuteExCMDAndWait(_cmdParams, RUN_AS_ADMIN, SHOW_WINDOW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);
end;

procedure mysqladminShutdown(pathMysqladmin: string; credentials: TCredentials;
  force: boolean = NOT_FORCE);
var
  _paramsMysqladmin: string;
  _mysqlIsShutDown: boolean;
begin
  _mysqlIsShutDown := false;
  if force then
  begin
    _mysqlIsShutDown := not checkMySQLCredentials(credentials);
  end;

  if (not _mysqlIsShutDown) then
  begin
    validateThatFileExists(pathMysqladmin);
    validateMySQLCredentials(credentials);
    _paramsMysqladmin := credentials.getMySQLCliCredentialsParams;
    _paramsMysqladmin := _paramsMysqladmin + ' shutdown';
    shellExecuteExe(pathMysqladmin, _paramsMysqladmin, SHOW_WINDOW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);
  end;
end;

procedure importScript(pathMysqlCli: string; fileNameIn: string; connectionString: string);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  importScript(pathMysqlCli, fileNameIn, _credentials);
end;

procedure mysql_upgrade(pathMysql_upgrade: string; connectionString: string; force: boolean = FALSE);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  mysql_upgrade(pathMysql_upgrade, _credentials, force);
end;

procedure mysqladminShutdown(pathMysqladmin: string; connectionString: string; force: boolean = NOT_FORCE);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  mysqladminShutdown(pathMysqladmin, _credentials, force);
end;

procedure TMysqldumpArgs.clear;
begin
  Self := Default (TMysqldumpArgs);
end;

end.
