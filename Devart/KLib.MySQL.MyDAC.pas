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

unit KLib.MySQL.MyDAC;

interface

uses
  KLib.MySQL.Info,
  KLib.MySQL.Credentials,
  MyAccess;

type
  T_Query = class(MyAccess.TMyQuery)
  public
    destructor Destroy; override;
  end;

  T_Connection = class(MyAccess.TMyConnection)
  private
    function _get_pooled: boolean;
    procedure _set_pooled(value: boolean);
    function _get_isAutoReconnectEnabled: boolean;
    procedure _set_isAutoReconnectEnabled(value: boolean);
  public
    property pooled: boolean read _get_pooled write _set_pooled;
    property isAutoReconnectEnabled: boolean read _get_isAutoReconnectEnabled write _set_isAutoReconnectEnabled;
    constructor Create(mySQLCredentials: TCredentials); reintroduce; overload;
      destructor Destroy; override;
  end;

function _getMySQLTConnection(mySQLCredentials: TCredentials): T_Connection;

function getValidMySQLTMyConnection(mySQLCredentials: TCredentials): TMyConnection;
function getMySQLTMyConnection(mySQLCredentials: TCredentials): TMyConnection;

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.Validate;

destructor T_Query.Destroy;
begin
  inherited;
end;

constructor T_Connection.Create(mySQLCredentials: TCredentials);
begin
  inherited Create(nil);
  with Self do
  begin
    Server := mysqlCredentials.server;
    Username := mysqlCredentials.credentials.username;
    Password := mysqlCredentials.credentials.password;
    Port := mysqlCredentials.port;
    Database := mysqlCredentials.database;
  end;
end;

function T_Connection._get_pooled: boolean;
begin
  Result := Self.Pooling;
end;

procedure T_Connection._set_pooled(value: boolean);
begin
  Self.Pooling := value;
end;

function T_Connection._get_isAutoReconnectEnabled: boolean;
begin
  Result := Self.Options.LocalFailover;
end;

procedure T_Connection._set_isAutoReconnectEnabled(value: boolean);
begin
  Self.Options.LocalFailover := value;
end;

destructor T_Connection.Destroy;
begin
  inherited;
end;

function _getMySQLTConnection(mySQLCredentials: TCredentials): T_Connection;
var
  connection: T_Connection;

  _MyConnection: TMyConnection;
begin
  _MyConnection := getMySQLTMyConnection(mySQLCredentials);
  connection := T_Connection(_MyConnection);

  Result := connection;
end;

function getValidMySQLTMyConnection(mySQLCredentials: TCredentials): TMyConnection;
var
  connection: TMyConnection;
begin
  validateMySQLCredentials(mySQLCredentials);
  connection := getMySQLTMyConnection(mySQLCredentials);

  Result := connection;
end;

function getMySQLTMyConnection(mySQLCredentials: TCredentials): TMyConnection;
var
  connection: TMyConnection;
begin
  validateRequiredMySQLProperties(mySQLCredentials);
  connection := TMyConnection.Create(nil);
  with connection do
  begin
    Server := mysqlCredentials.server;
    Username := mysqlCredentials.credentials.username;
    Password := mysqlCredentials.credentials.password;
    Port := mysqlCredentials.port;
    Database := mysqlCredentials.database;
  end;

  Result := connection;
end;

end.
