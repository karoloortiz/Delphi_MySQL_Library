{
  KLib Version = 4.0
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

unit KLib.MySQL.FireDac;

interface

uses
  KLib.MySQL.Credentials,
  FireDAC.Comp.Client;

type
  T_Query = class(FireDAC.Comp.Client.TFDQuery)
  public
    destructor Destroy; override;
  end;

  T_Connection = class(FireDAC.Comp.Client.TFDConnection)
  private
    isSSLUsed: boolean;
    function _get_database: string;
    procedure _set_database(value: string);
    function _get_port: integer;
    procedure _set_port(value: integer);
    function _get_pooled: boolean;
    procedure _set_pooled(value: boolean);
    function _get_isAutoReconnectEnabled: boolean;
    procedure _set_isAutoReconnectEnabled(value: boolean);
  protected
    procedure DoConnect; override;
  public
    property database: string read _get_database write _set_database;
    property port: integer read _get_port write _set_port;
    property pooled: boolean read _get_pooled write _set_pooled;
    property isAutoReconnectEnabled: boolean read _get_isAutoReconnectEnabled write _set_isAutoReconnectEnabled;
    constructor Create(credentials: TCredentials); reintroduce; overload;
    destructor Destroy; override;
  end;

function _getMySQLTConnection(credentials: TCredentials): T_Connection;

procedure getMySQLClientDLLFromResourceIfNotExists();
//procedure deleteMySQLClientDLLIfExists; //TODO UNLOAD DLL

implementation

uses
  System.SysUtils,
  FireDAC.VCLUI.Wait,
  FireDAC.Stan.Def, FireDAC.Stan.Async,
  FireDac.DApt,
  FireDAC.Phys.MySQLDef, FireDAC.Phys.MySQL,
  Winapi.Windows,
  Klib.Utils, KLib.Windows, KLib.FileSystem,
  KLib.MySQL.Utils, KLib.MySQL.Validate, KLib.MySQL.FireDac.Resources;

var
  driverLink: TFDPhysMySQLDriverLink = nil;

procedure setMariaDBPeerVerification(isEnabled: boolean); forward;
procedure getCachingSha2PasswordDLLFromResourceIfNotExists(); forward;

destructor T_Query.Destroy;
begin
  inherited;
end;

constructor T_Connection.Create(credentials: TCredentials);
begin
  inherited Create(nil);

  LoginPrompt := false;
  DriverName := 'MySQL';
  Params.Values['Server'] := credentials.server;
  Params.Values['User_Name'] := credentials.credentials.username;
  Params.Values['Password'] := credentials.credentials.password;
  Params.Values['Port'] := IntToStr(credentials.port);
  Params.Values['Database'] := credentials.database;
  Params.Values['CharacterSet'] := CHARSET_NAMES[credentials.charset];
  isSSLUsed := credentials.useSSL;
  if (isSSLUsed) then
  begin
    Params.Values['UseSSL'] := 'True';
  end;
end;

procedure T_Connection.DoConnect;
begin
  setMariaDBPeerVerification(isSSLUsed);
  try
    inherited;
  finally
    setMariaDBPeerVerification(true);
  end;
end;

function T_Connection._get_database: string;
begin
  Result := TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Database;
end;

procedure T_Connection._set_database(value: string);
begin
  TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Database := value;
end;

function T_Connection._get_port: integer;
begin
  Result := TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Port;
end;

procedure T_Connection._set_port(value: integer);
begin
  TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Port := value;
end;

function T_Connection._get_pooled: boolean;
begin
  Result := TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Pooled;
end;

procedure T_Connection._set_pooled(value: boolean);
begin
  TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Pooled := value;
end;

function T_Connection._get_isAutoReconnectEnabled: boolean;
begin
  Result := Self.ResourceOptions.AutoReconnect;
end;

procedure T_Connection._set_isAutoReconnectEnabled(value: boolean);
begin
  Self.ResourceOptions.AutoReconnect := value;
end;

destructor T_Connection.Destroy;
begin
  inherited;
end;

function _getMySQLTConnection(credentials: TCredentials): T_Connection;
var
  connection: T_Connection;
begin
  validateRequiredMySQLProperties(credentials);
  getMySQLClientDLLFromResourceIfNotExists();
  if (credentials.use_caching_sha2_password_dll) then
  begin
    getCachingSha2PasswordDLLFromResourceIfNotExists();
  end;
  connection := T_Connection.Create(credentials);

  Result := connection;
end;

//Since MariaDB Connector/C 3.4 TLS is enforced and the server certificate is verified on every non-local connection.
procedure setMariaDBPeerVerification(isEnabled: boolean);
const
  ENV_VAR_MARIADB_TLS_DISABLE_PEER_VERIFICATION = 'MARIADB_TLS_DISABLE_PEER_VERIFICATION';
var
  _value: PChar;
begin
  if (isEnabled) then
  begin
    _value := nil;
  end
  else
  begin
    _value := '1';
  end;
  SetEnvironmentVariable(ENV_VAR_MARIADB_TLS_DISABLE_PEER_VERIFICATION, _value);
end;

const
  FILENAME_LIBMARIADB = 'libmariadb.dll';

procedure getMySQLClientDLLFromResourceIfNotExists();
var
  _pathLibmariadb: string;
begin
  _pathLibmariadb := getCombinedPathWithCurrentDir(FILENAME_LIBMARIADB);
  if (not FileExists(_pathLibmariadb)) then
  begin
    getResourceAsFile(RESOURCE_LIBMARIADB, _pathLibmariadb);
  end;

  if (driverLink = nil) then
  begin
    driverLink := TFDPhysMySQLDriverLink.Create(nil);
  end;
  driverLink.VendorLib := _pathLibmariadb;
end;

//The caching_sha2_password plugin is dynamic in MariaDB Connector/C, libmariadb loads it by name from the exe dir.
procedure getCachingSha2PasswordDLLFromResourceIfNotExists();
const
  FILENAME_CACHING_SHA2_PASSWORD = 'caching_sha2_password.dll';
var
  _pathCachingSha2Password: string;
begin
  _pathCachingSha2Password := getCombinedPathWithCurrentDir(FILENAME_CACHING_SHA2_PASSWORD);
  if (not FileExists(_pathCachingSha2Password)) then
  begin
    getResourceAsFile(RESOURCE_CACHING_SHA2_PASSWORD, _pathCachingSha2Password);
  end;
end;

//TODO UNLOAD DLL
//procedure deleteMySQLClientDLLIfExists;
//var
//  _path_libmariadb: string;
//  _path_libmysql: string;
//  FLib: TMySQLLib;
//begin
//{$ifdef WIN32}
//  _path_libmysql := getCombinedPathWithCurrentDir(FILENAME_LIBMYSQL);
//  deleteFileIfExists(_path_libmysql);
//{$else IFDEF WIN64}
//  _path_libmariadb := getCombinedPathWithCurrentDir(FILENAME_LIBMARIAB);
//  deleteFileIfExists(_path_libmariadb);
//{$endif}
//end;

initialization

finalization

FreeAndNil(driverLink);

end.
