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

unit KLib.MySQL.Utils;

interface

uses
  KLib.MySQL.DriverPort, KLib.MySQL.Info,
  System.Classes;

procedure MyISAMToInnoDBInDumpFile(filename: string; filenameOutput: string = '');

function checkIfMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials): boolean;
function getMySQLVersion(mySQLCredentials: TMySQLCredentials): TMySQLVersion;
function getMySQLVersionAsString(mySQLCredentials: TMySQLCredentials): string;
function getNonStandardsDatabasesAsStringList(mySQLCredentials: TMySQLCredentials): TStringList;
function getMySQLDataDir(mySQLCredentials: TMySQLCredentials): string;
function getFirstFieldListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant;
function getFirstFieldFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; connection: TConnection): Variant; overload;

function getSQLStatementWithFieldInserted(sqlStatement: string; fieldName: string): string;
function getSQLStatementWithWhereStmtInserted(sqlStatement: string; fieldName: string): string;

procedure emptyTable(tableName: string; connection: TConnection);

procedure executeQuery(sqlStatement: string; connection: TConnection);

function checkMySQLCredentials(mySQLCredentials: TMySQLCredentials): boolean;
function checkRequiredMySQLProperties(mySQLCredentials: TMySQLCredentials): boolean;

procedure cleanDataDir_v5_7(pathDataDir: string);

implementation

uses
  KLib.Validate, KLib.MyString, KLib.Utils,
  System.SysUtils, System.StrUtils, System.Variants;

procedure MyISAMToInnoDBInDumpFile(filename: string; filenameOutput: string = '');
var
  _file: TStringList;
  _stringBuilder: TStringBuilder;

  _filenameOutput: string;
begin
  validateThatFileExists(filename);

  _filenameOutput := filenameOutput;
  if _filenameOutput = '' then
  begin
    _filenameOutput := filename;
  end;
  _file := TStringList.Create;
  _file.LoadFromFile(filename);
  _stringBuilder := TStringBuilder.Create;
  _stringBuilder.Append(_file.Text);
  _stringBuilder.Replace('ENGINE=MyISAM', 'ENGINE=InnoDB');
  _file.Clear;
  _file.Text := _stringBuilder.ToString;
  _file.SaveToFile(_filenameOutput);
  FreeAndNil(_file);
  FreeAndNil(_stringBuilder);
end;

function checkIfMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials): boolean;
var
  _version: TMySQLVersion;
  _result: boolean;
begin
  _version := getMySQLVersion(mySQLCredentials);
  _result := _version = TMySQLVersion.v_8;
  Result := _result;
end;

function getMySQLVersion(mySQLCredentials: TMySQLCredentials): TMySQLVersion;
const
  MYSQL_V5_5 = '5.5';
  MYSQL_V5_7 = '5.7';
  MYSQL_V8 = '8';

  ERR_MSG = 'Unknown version of MySQL.';
var
  _versionAsString: string;
  version: TMySQLVersion;
begin
  _versionAsString := getMySQLVersionAsString(mySQLCredentials);
  if AnsiStartsStr(MYSQL_V5_5, _versionAsString) then
  begin
    version := TMySQLVersion.v5_5;
  end
  else if AnsiStartsStr(MYSQL_V5_7, _versionAsString) then
  begin
    version := TMySQLVersion.v5_7;
  end
  else if AnsiStartsStr(MYSQL_V8, _versionAsString) then
  begin
    version := TMySQLVersion.v_8;
  end
  else
  begin
    raise Exception.Create(ERR_MSG);
  end;
  result := version;
end;

function getMySQLVersionAsString(mySQLCredentials: TMySQLCredentials): string;
const
  SQL_STATEMENT = 'select @@version';
var
  version: string;
begin
  version := getFirstFieldFromSQLStatement(SQL_STATEMENT, mySQLCredentials);
  result := version;
end;

function getNonStandardsDatabasesAsStringList(mySQLCredentials: TMySQLCredentials): TStringList;
const
  SQL_STATEMENT = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN (''information_schema'', ''mysql'', ''performance_schema'',''sys'')';
var
  _databases: Variant;
  databases: TStringList;
  i: integer;
  highBound: integer;
begin
  _databases := getFirstFieldListFromSQLStatement(SQL_STATEMENT, mySQLCredentials);
  databases := TStringList.Create;

  highBound := VarArrayHighBound(_databases, 1);
  for i := VarArrayLowBound(_databases, 1) to highBound do
  begin
    databases.Add(_databases[i]);
  end;

  result := databases;
end;

function getMySQLDataDir(mySQLCredentials: TMySQLCredentials): string;
const
  SQL_STATEMENT = 'select @@datadir';
var
  dataDir: string;
begin
  dataDir := getFirstFieldFromSQLStatement(SQL_STATEMENT, mySQLCredentials);
  result := dataDir;
end;

function getFirstFieldListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant;
var
  _connection: TConnection;
  _query: TQuery;
  fieldListResult: variant;
  i: integer;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  _connection.Connected := true;
  _query := getTQuery(_connection, sqlStatement);

  _query.open;
  fieldListResult := VarArrayCreate([0, _query.RecordCount - 1], varVariant);
  for i := 0 to _query.RecordCount - 1 do
  begin
    fieldListResult[i] := _query.FieldList.Fields[0].value;
    _query.Next;
  end;

  _query.Close;
  _connection.Connected := false;
  FreeAndNil(_connection);
  FreeAndNil(_query);

  result := fieldListResult;
end;

function getFirstFieldFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant;
var
  _connection: TConnection;
  fieldResult: variant;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  _connection.Connected := true;

  fieldResult := getFirstFieldFromSQLStatement(sqlStatement, _connection);

  _connection.Connected := false;
  FreeAndNil(_connection);

  result := fieldResult;
end;

function getFirstFieldFromSQLStatement(sqlStatement: string; connection: TConnection): Variant;
var
  _query: TQuery;
  fieldResult: variant;
begin
  _query := getTQuery(connection, sqlStatement);
  _query.open;
  fieldResult := _query.FieldList.Fields[0].value;
  _query.Close;
  FreeAndNil(_query);

  result := fieldResult;
end;

function getSQLStatementWithFieldInserted(sqlStatement: string; fieldName: string): string;
var
  _result: string;
  _lastFieldPos: integer;
  _tempQueryStmt: string;
  _insertedString: string;
begin
  _tempQueryStmt := UpperCase(sqlStatement);
  _lastFieldPos := AnsiPos('FROM', _tempQueryStmt) - 1;
  _insertedString := ', ' + fieldName + ' ';
  _result := getMainStringWithSubStringInserted(sqlStatement, _insertedString, _lastFieldPos);

  Result := _result;
end;

function getSQLStatementWithWhereStmtInserted(sqlStatement: string; fieldName: string): string;
var
  _result: string;
  _lastFieldPos: integer;
  _tempQueryStmt: string;
  _insertedString: string;
begin
  _tempQueryStmt := UpperCase(sqlStatement);
  _lastFieldPos := AnsiPos('ORDER', _tempQueryStmt) - 1;
  if _lastFieldPos = -1 then
  begin
    _lastFieldPos := AnsiPos('LIMIT', _tempQueryStmt) - 1;
    if _lastFieldPos = -1 then
    begin
      _lastFieldPos := Length(_tempQueryStmt);
    end;
  end;
  _insertedString := ' ' + fieldName + ' ';
  _result := getMainStringWithSubStringInserted(sqlStatement, _insertedString, _lastFieldPos);

  Result := _result;
end;

procedure emptyTable(tableName: string; connection: TConnection);
const
  PARAM_TABLENAME = ':TABLENAME';
  DELETE_FROM_WHERE_PARAM_TABLENAME =
    'DELETE' + sLineBreak +
    'FROM' + sLineBreak +
    PARAM_TABLENAME;
var
  _queryStmt: myString;
begin
  _queryStmt := DELETE_FROM_WHERE_PARAM_TABLENAME;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);
  executeQuery(_queryStmt, connection);
end;

procedure executeQuery(sqlStatement: string; connection: TConnection);
var
  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  _query.ExecSQL;
  FreeAndNil(_query);
end;

function checkMySQLCredentials(mySQLCredentials: TMySQLCredentials): boolean;
var
  _connection: TConnection;
  _result: boolean;
begin
  _connection := getMySQLTConnection(mySQLCredentials);
  try
    _connection.Connected := true;
    _result := true;
  except
    on E: Exception do
    begin
      _result := false;
    end;
  end;
  _connection.Connected := false;
  _connection.Free;

  Result := _result;
end;

function checkRequiredMySQLProperties(mySQLCredentials: TMySQLCredentials): boolean;
var
  _result: boolean;
begin
  with mySQLCredentials do
  begin
    _result := (server <> '') and (credentials.username <> '') and (credentials.password <> '') and (port <> 0);
  end;

  Result := _result;
end;

procedure cleanDataDir_v5_7(pathDataDir: string);
const
  IBDATA1_FILENAME = 'ibdata1';
var
  _fileNamesList: TStringList;
  _fileName: string;
  _nameOfFile: string;
begin
  validateThatDirExists(pathDataDir);
  _fileNamesList := getFileNamesListInDir(pathDataDir);
  try
    for _fileName in _fileNamesList do
    begin
      _nameOfFile := ExtractFileName(_fileName);
      if _nameOfFile <> IBDATA1_FILENAME then
      begin
        deleteFileIfExists(_fileName);
      end;
    end;
  finally
    FreeAndNil(_fileNamesList);
  end;
end;

end.
