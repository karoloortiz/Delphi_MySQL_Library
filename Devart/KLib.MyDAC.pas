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

unit KLib.MyDAC;

interface

uses
  KLib.MySQL.Info,
  MyAccess;

type
  TQuery = class(MyAccess.TMyQuery)
  end;

  TConnection = class(MyAccess.TMyConnection)
  end;

function getValidMySQLTConnection_(mySQLCredentials: TMySQLCredentials): TConnection;
function getMySQLTConnection_(mySQLCredentials: TMySQLCredentials): TConnection;

function getValidMySQLTMyConnection(mySQLCredentials: TMySQLCredentials): TMyConnection;
function getMySQLTMyConnection(mySQLCredentials: TMySQLCredentials): TMyConnection;

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.Validate;

function getValidMySQLTConnection_(mySQLCredentials: TMySQLCredentials): TConnection;
var
  _MyConnection: TMyConnection;
  connection: TConnection;
begin
  _MyConnection := getValidMySQLTMyConnection(mySQLCredentials);
  connection := TConnection(_MyConnection);
  Result := connection;
end;

function getMySQLTConnection_(mySQLCredentials: TMySQLCredentials): TConnection;
var
  _MyConnection: TMyConnection;
  connection: TConnection;
begin
  _MyConnection := getMySQLTMyConnection(mySQLCredentials);
  connection := TConnection(_MyConnection);
  Result := connection;
end;

function getValidMySQLTMyConnection(mySQLCredentials: TMySQLCredentials): TMyConnection;
var
  connection: TMyConnection;
begin
  validateMySQLCredentials(mySQLCredentials);
  connection := getMySQLTMyConnection(mySQLCredentials);
  Result := connection;
end;

function getMySQLTMyConnection(mySQLCredentials: TMySQLCredentials): TMyConnection;
var
  connection: TMyConnection;
begin
  validateRequiredMySQLProperties(mySQLCredentials);
  connection := TMyConnection.Create(nil);
  with mySQLCredentials do
  begin
    connection.Server := server;
    with credentials do
    begin
      connection.Username := username;
      connection.Password := password;
    end;
    connection.Port := port;
    if database <> '' then
    begin
      connection.Database := database;
    end;
  end;
  Result := connection;
end;

end.
