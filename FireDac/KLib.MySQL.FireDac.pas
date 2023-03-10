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

unit KLib.MySQL.FireDac;

interface

uses
  KLib.MySQL.Info,
  FireDAC.Comp.Client;

type
  T_Query = class(FireDAC.Comp.Client.TFDQuery)
  public
    destructor Destroy; override;
  end;

  T_Connection = class(FireDAC.Comp.Client.TFDConnection)
  private
    function _get_database: string;
    procedure _set_database(value: string);
    function _get_port: integer;
    procedure _set_port(value: integer);
    function _get_pooled: boolean;
    procedure _set_pooled(value: boolean);
  public
    property database: string read _get_database write _set_database;
    property port: integer read _get_port write _set_port;
    property pooled: boolean read _get_pooled write _set_pooled;

    constructor Create(mySQLCredentials: TMySQLCredentials); reintroduce; overload;
    destructor Destroy; override;
  end;

function _getMySQLTConnection(mySQLCredentials: TMySQLCredentials): T_Connection;

procedure getMySQLClientDLLFromResourceIfNotExists;
//procedure deleteMySQLClientDLLIfExists; //TODO UNLOAD DLL

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.Validate, KLib.MySQL.FireDac.Resources,
  Klib.Utils, KLib.Windows,
  FireDAC.VCLUI.Wait,
  FireDAC.Stan.Def, FireDAC.Stan.Async,
  FireDac.DApt,
  FireDAC.Phys.MySQLDef, FireDAC.Phys.MySQL,
  System.SysUtils;

destructor T_Query.Destroy;
begin
  inherited;
end;

constructor T_Connection.Create(mySQLCredentials: TMySQLCredentials);
begin
  inherited Create(nil);

  LoginPrompt := false;
  DriverName := 'MySQL';
  with Params do
  begin
    Values['Server'] := mySQLCredentials.server;
    Values['User_Name'] := mySQLCredentials.credentials.username;
    Values['Password'] := mySQLCredentials.credentials.password;
    Values['Port'] := IntToStr(mySQLCredentials.port);
    Values['Database'] := mySQLCredentials.database;
    if (mySQLCredentials.useSSL) then
    begin
      Values['UseSSL'] := 'True';
    end;
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

destructor T_Connection.Destroy;
begin
  inherited;
end;

function _getMySQLTConnection(mySQLCredentials: TMySQLCredentials): T_Connection;
var
  connection: T_Connection;
begin
  validateRequiredMySQLProperties(mySQLCredentials);
  getMySQLClientDLLFromResourceIfNotExists;
  connection := T_Connection.Create(mySQLCredentials);

  Result := connection;
end;

const
  FILENAME_LIBMARIAB = 'libmariadb.dll';
  FILENAME_LIBMYSQL = 'libmysql.dll';

procedure getMySQLClientDLLFromResourceIfNotExists;
var
  _path_dll: string;
begin
{$ifdef WIN32}
  _path_dll := getCombinedPathWithCurrentDir(FILENAME_LIBMYSQL);
  if not FileExists(_path_dll) then
  begin
    getResourceAsFile(RESOURCE_LIBMYSQL, _path_dll);
  end;
{$else IFDEF WIN64}
  _path_dll := getCombinedPathWithCurrentDir(FILENAME_LIBMARIAB);
  if not FileExists(_path_dll) then
  begin
    getResourceAsFile(RESOURCE_LIBMARIADB, _path_dll);
  end;
{$endif}
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

end.
