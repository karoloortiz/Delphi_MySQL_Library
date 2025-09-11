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

unit KLib.MySQL.Driver;

interface

//##############################################################################
//If you want to have global defines in (IDE -> Project -> Options -> Conditional Defines) adds
//  KLIB_GLOBALS
//FOR FIREDAC
//  KLIB_MYSQL_FIREDAC
//FOR MYDAC
//
//##############################################################################
{$ifndef KLIB_GLOBALS}
{$include KLib.MySQL.inc}
{$ifend}


uses
  //----------------------------------------------------------------------------
{$ifdef KLIB_MYSQL_FIREDAC}
  KLib.MySQL.FireDac,
{$else}
{$ifdef KLIB_MYSQL_MYDAC}
  KLib.MySQL.MyDAC,
{$ifend}
{$ifend}
  //----------------------------------------------------------------------------
  KLib.MySQL.Info, KLib.MySQL.Credentials,
  System.Classes;

type
  TQuery = class(T_Query)
  public
    procedure refreshKeepingPosition;
    destructor Destroy; override;
  end;

  TConnection = class(T_Connection)
  public
    function checkIfMysqlVersionIs_v_8: boolean;
    function getMySQLVersion: TMySQLVersion;
    function getMySQLVersionAsString: string;
    function getNonStandardsDatabasesAsStringList: TStringList;
    function getMySQLDataDir: string;
    function getFirstFieldListFromSQLStatement(sqlStatement: string): Variant;
    function getFirstFieldFromSQLStatement(sqlStatement: string): Variant;

    procedure emptyTable(tableName: string);

    procedure executeScript(scriptSQL: string);
    procedure executeQuery(sqlStatement: string);

    function getACopyConnection: TConnection;
    destructor Destroy; override;
  end;

function getTQuery(credentials: TCredentials; sqlText: string = ''): TQuery; overload;
function getTQuery(connection: TConnection; sqlText: string = ''): TQuery; overload;

function getValidTConnection(credentials: TCredentials): TConnection;
function getTConnection(credentials: TCredentials): TConnection;

implementation

uses
  KLib.MySQL.Validate, KLib.MySQL.Utils, KLib.MySQL.Resources,
  Klib.Windows, KLib.Utils,
  Data.DB,
  System.SysUtils;

function TConnection.checkIfMysqlVersionIs_v_8: boolean;
begin
  Result := KLib.MySQL.Utils.checkIfMysqlVersionIs_v_8(Self);
end;

function TConnection.getMySQLVersion: TMySQLVersion;
begin
  Result := KLib.MySQL.Utils.getMySQLVersion(Self);
end;

function TConnection.getMySQLVersionAsString: string;
begin
  Result := KLib.MySQL.Utils.getMySQLVersionAsString(Self);
end;

function TConnection.getNonStandardsDatabasesAsStringList: TStringList;
begin
  Result := KLib.MySQL.Utils.getNonStandardsDatabasesAsStringList(Self);
end;

function TConnection.getMySQLDataDir: string;
begin
  Result := KLib.MySQL.Utils.getMySQLDataDir(Self);
end;

function TConnection.getFirstFieldListFromSQLStatement(sqlStatement: string): Variant;
begin
  Result := KLib.MySQL.Utils.getFirstFieldListFromSQLStatement(sqlStatement, Self);
end;

function TConnection.getFirstFieldFromSQLStatement(sqlStatement: string): Variant;
begin
  Result := KLib.MySQL.Utils.getFirstFieldFromSQLStatement(sqlStatement, Self);
end;

procedure TConnection.emptyTable(tableName: string);
begin
  KLib.MySQL.Utils.emptyTable(tableName, Self);
end;

procedure TConnection.executeScript(scriptSQL: string);
begin
  KLib.MySQL.Utils.executeScript(scriptSQL, Self);
end;

procedure TConnection.executeQuery(sqlStatement: string);
begin
  KLib.MySQL.Utils.executeQuery(sqlStatement, Self);
end;

function TConnection.getACopyConnection: TConnection;
var
  connection: TConnection;
begin
  connection := TConnection.Create(nil);
  connection.Assign(Self);

  Result := connection;
end;

procedure TQuery.refreshKeepingPosition;
begin
  refreshQueryKeepingPosition(Self);
end;

destructor TQuery.Destroy;
begin
  inherited;
end;

destructor TConnection.Destroy;
begin
  inherited;
end;

function getTQuery(credentials: TCredentials; sqlText: string = ''): TQuery;
var
  query: TQuery;

  _connection: TConnection;
begin
  _connection := getTConnection(credentials);
  try
    _connection.Connected := true;
    query := getTQuery(_connection, sqlText);
    _connection.Connected := false;
  finally
    _connection.Free;
  end;

  Result := query;
end;

function getTQuery(connection: TConnection; sqlText: string = ''): TQuery;
var
  query: TQuery;
begin
  query := TQuery.create(nil);
  query.connection := connection;
  query.SQL.Clear;
  query.SQL.Text := sqlText;

  Result := query;
end;

function getValidTConnection(credentials: TCredentials): TConnection;
var
  connection: TConnection;
begin
  validateMySQLCredentials(credentials);
  connection := getTConnection(credentials);

  Result := connection;
end;

procedure getCaching_sha2_passwordDLLFromResourceIfNotExists(); forward;

function getTConnection(credentials: TCredentials): TConnection;
var
  connection: T_Connection;
begin
  if credentials.use_caching_sha2_password_dll then
  begin
    getCaching_sha2_passwordDLLFromResourceIfNotExists();
  end;
  connection := _getMySQLTConnection(credentials);

  Result := TConnection(connection);
end;

procedure getCaching_sha2_passwordDLLFromResourceIfNotExists();
const
  FILENAME_DLL = 'caching_sha2_password.dll';
var
  _path_dll: string;
begin
  _path_dll := getCombinedPathWithCurrentDir(FILENAME_DLL);
  if not FileExists(_path_dll) then
  begin
    getResourceAsFile(RESOURCE_CACHING_SHA2_PASSWORD, _path_dll);
  end;
end;

end.
