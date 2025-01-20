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

unit KLib.MySQL.TemporaryTable;

interface

uses
  KLib.MySQL.Driver,
  KLib.Constants;

type
  TTemporaryTable = class
  private
    _connection: TConnection;
    _selectQueryStmt: string;
    _createdStatus: boolean;
  public
    tableName: String;
    property createdStatus: boolean read _createdStatus;

    constructor create(connection: TConnection; tableName: string = EMPTY_STRING; selectQueryStmt: string = EMPTY_STRING);
    procedure recreate(selectQueryStmt: string; tableName: string = EMPTY_STRING; connection: TConnection = nil);
    procedure initialize(tableName: string; connection: TConnection = nil; selectQueryStmt: string = EMPTY_STRING);
    procedure execute;
    procedure drop;
    destructor Destroy; override;
  end;

function getCreateTemporaryTableFromQuery_SQLStmt(tableName: string; queryStmt: string): string;
function getDropTemporaryTable_SQLStmt(tableName: string): string;

implementation

uses
  KLib.MySQL.Utils,
  KLib.sqlstring,
  System.SysUtils;

function getCreateTemporaryTableFromQuery_SQLStmt(tableName: string; queryStmt: string): string;
const
  PARAM_TABLENAME = ':TABLENAME';
  PARAM_QUERYSTMT = ':QUERYSTMT';
  CREATE_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME_PARAM_QUERYSTMT =
    'CREATE TEMPORARY TABLE' + sLineBreak +
    PARAM_TABLENAME + sLineBreak +
    PARAM_QUERYSTMT;
var
  _queryStmt: sqlstring;
begin
  _queryStmt := CREATE_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME_PARAM_QUERYSTMT;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);
  _queryStmt.setParamAsString(PARAM_QUERYSTMT, queryStmt);

  Result := _queryStmt;
end;

function getDropTemporaryTable_SQLStmt(tableName: string): string;
const
  PARAM_TABLENAME = ':TABLENAME';
  DROP_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME =
    'DROP TEMPORARY TABLE' + sLineBreak +
    PARAM_TABLENAME;
var
  _queryStmt: sqlstring;
begin
  _queryStmt := DROP_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);

  Result := _queryStmt;
end;

constructor TTemporaryTable.create(connection: TConnection; tableName: string = EMPTY_STRING; selectQueryStmt: string = EMPTY_STRING);
begin
  initialize(tableName, connection, selectQueryStmt);
end;

procedure TTemporaryTable.recreate(selectQueryStmt: string; tableName: string = EMPTY_STRING; connection: TConnection = nil);
var
  _tableName: string;
begin
  Self.drop;

  _tableName := tableName;
  if (_tableName = EMPTY_STRING) then
  begin
    _tableName := Self.tableName;
  end;
  initialize(_tableName, connection, selectQueryStmt);
  execute;
end;

procedure TTemporaryTable.initialize(tableName: string; connection: TConnection = nil; selectQueryStmt: string = EMPTY_STRING);
begin
  Self.tableName := tableName;
  Self._selectQueryStmt := selectQueryStmt;
  if (connection <> nil) then
  begin
    Self._connection := connection;
  end;
  _createdStatus := false;
end;

procedure TTemporaryTable.execute;
const
  ERR_MSG = 'Temporary table already created.';
var
  _queryStmt: string;
begin
  if createdStatus then
  begin
    raise Exception.Create(ERR_MSG);
  end;
  _queryStmt := getCreateTemporaryTableFromQuery_SQLStmt(tableName, _selectQueryStmt);
  executeQuery(_queryStmt, _connection);

  _createdStatus := true;
end;

procedure TTemporaryTable.drop;
var
  _queryStmt: string;
begin
  if createdStatus then
  begin
    _queryStmt := getDropTemporaryTable_SQLStmt(tableName);
    executeQuery(_queryStmt, _connection);
    _createdStatus := false;
  end;
end;

destructor TTemporaryTable.destroy;
begin
  drop;
  inherited;
end;

end.
