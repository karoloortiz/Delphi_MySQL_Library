{
  KLib Version = 1.0
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
  end;

  T_Connection = class(FireDAC.Comp.Client.TFDConnection)
  private
    function getPort: integer;
    procedure setport(value: integer);
  public
    property port: integer read getPort write setPort;
  end;

function _getMySQLTConnection(mySQLCredentials: TMySQLCredentials): T_Connection;

function getValidMySQLTFDConnection(mySQLCredentials: TMySQLCredentials): TFDConnection;
function getMySQLTFDConnection(mySQLCredentials: TMySQLCredentials): TFDConnection;
procedure getMySQLClientDLLFromResourceIfNotExists;
//procedure deleteMySQLClientDLLIfExists; //TODO UNLOAD DLL

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.Validate, KLib.MySQL.FireDac.Resources,
  Klib.Utils,
  FireDAC.VCLUI.Wait,
  FireDAC.Stan.Def, FireDAC.Stan.Async,
  FireDac.DApt,
  FireDAC.Phys.MySQLDef, FireDAC.Phys.MySQL,
  System.SysUtils;

function T_Connection.getPort: integer;
begin
  Result := TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Port;
end;

procedure T_Connection.setPort(value: integer);
begin
  TFDPhysMySQLConnectionDefParams(ResultConnectionDef.Params).Port := value;
end;

function _getMySQLTConnection(mySQLCredentials: TMySQLCredentials): T_Connection;
var
  _FDConnection: TFDConnection;
  connection: T_Connection;
begin
  _FDConnection := getMySQLTFDConnection(mySQLCredentials);
  connection := T_Connection(_FDConnection);
  Result := connection;
end;

function getValidMySQLTFDConnection(mySQLCredentials: TMySQLCredentials): TFDConnection;
var
  connection: TFDConnection;
begin
  validateMySQLCredentials(mySQLCredentials);
  connection := getMySQLTFDConnection(mySQLCredentials);
  Result := connection;
end;

function getMySQLTFDConnection(mySQLCredentials: TMySQLCredentials): TFDConnection;
var
  connection: TFDConnection;
begin
  validateRequiredMySQLProperties(mySQLCredentials);
  getMySQLClientDLLFromResourceIfNotExists;
  connection := TFDConnection.Create(nil);
  with connection do
  begin
    LoginPrompt := false;

    DriverName := 'MySQL';
    with Params do
    begin
      with mySQLCredentials do
      begin
        Values['Server'] := server;
        with credentials do
        begin
          Values['User_Name'] := username;
          Values['Password'] := password;
        end;
        Values['Port'] := IntToStr(port);
        Values['Database'] := database;
      end;
    end;
  end;
  Result := connection;
end;

const
  FILENAME_LIBMARIAB = 'libmariadb.dll';
  FILENAME_LIBMYSQL = 'libmysql.dll';

procedure getMySQLClientDLLFromResourceIfNotExists;
var
  _path_libmariadb: string;
  _path_libmysql: string;
begin
{$ifdef WIN32}
  _path_libmysql := getCombinedPathWithCurrentDir(FILENAME_LIBMYSQL);
  if not FileExists(_path_libmysql) then
  begin
    getResourceAsFile(RESOURCE_LIBMYSQL, _path_libmysql);
  end;
{$else IFDEF WIN64}
  _path_libmariadb := getCombinedPathWithCurrentDir(FILENAME_LIBMARIAB);
  if not FileExists(_path_libmariadb) then
  begin
    getResourceAsFile(RESOURCE_LIBMARIADB, _path_libmariadb);
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
