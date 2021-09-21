{
  KLib Version = 2.0
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

unit KLib.MySQL.DriverPort;

interface

uses
  //############################################################################
  // SELECT FIREDAC OR MYDAC.
  // FIREDAC IS AVAILABLE ON COMMUNITY EDITION
  //----------------------------------------------------------------------------
  KLib.FireDac,
  //  KLib.MyDac,
  //----------------------------------------------------------------------------
  //############################################################################
  KLib.MySQL.Info;

type
  TQuery = class(T_Query)
  public
    procedure refreshKeepingPosition;
  end;

  TConnection = class(T_Connection)
  end;

procedure refreshQueryKeepingPosition(query: TQuery);

function getTQuery(connection: TConnection; sqlText: string = ''): TQuery;

function getValidMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;
function getMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;

implementation

uses
  KLib.MySQL.Validate,
  Data.DB;

procedure TQuery.refreshKeepingPosition;
begin
  refreshQueryKeepingPosition(self);
end;

procedure refreshQueryKeepingPosition(query: TQuery);
var
  _bookmark: TBookmark;
begin
  _bookmark := Query.GetBookmark;
  query.Refresh;
  query.GotoBookmark(_bookmark);
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

function getValidMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;
var
  connection: TConnection;
begin
  validateMySQLCredentials(mySQLCredentials);
  connection := getMySQLTConnection(mySQLCredentials);
  Result := connection;
end;

function getMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;
var
  connection: T_Connection;
begin
  connection := _getMySQLTConnection(mySQLCredentials);
  Result := TConnection(connection);
end;

end.
