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

unit KLib.MySQL.DriverPort;

interface

uses
  //############################################################################
  // SELECT FIREDAC OR MYDAC.
  // FIREDAC IS AVAILABLE ON COMMUNITY EDITION
  //----------------------------------------------------------------------------
  //  KLib.MySQL.FireDac,
  KLib.MySQL.MyDAC,
  //----------------------------------------------------------------------------
  //############################################################################
  KLib.MySQL.Info,
  System.Classes;

type
  TQuery = class(T_Query)
  public
    procedure refreshKeepingPosition;
    destructor Destroy; override;
  end;

  TConnection = class(T_Connection)
  public
    function checkIfMysqlVersionIs_v_8: boolean; virtual;
    function getMySQLVersion: TMySQLVersion; virtual;
    function getMySQLVersionAsString: string; virtual;
    function getNonStandardsDatabasesAsStringList: TStringList; virtual;
    function getMySQLDataDir: string; virtual;
    function getFirstFieldListFromSQLStatement(sqlStatement: string): Variant; virtual;
    function getFirstFieldFromSQLStatement(sqlStatement: string): Variant; virtual;

    procedure emptyTable(tableName: string); virtual;

    procedure executeScript(scriptSQL: string); virtual;
    procedure executeQuery(sqlStatement: string); virtual;

    function getACopyConnection: TConnection; virtual;
    destructor Destroy; override;
  end;

procedure refreshQueryKeepingPosition(query: TQuery);

function getTQuery(connection: TConnection; sqlText: string = ''): TQuery;

function getValidMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;
function getMySQLTConnection(mySQLCredentials: TMySQLCredentials): TConnection;

implementation

uses
  KLib.MySQL.Validate, KLib.MySQL.Utils,
  Data.DB;

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

procedure refreshQueryKeepingPosition(query: TQuery);
var
  _bookmark: TBookmark;
begin
  _bookmark := query.GetBookmark;
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
