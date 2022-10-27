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

unit KLib.MySQL.Utils;

interface

uses
  KLib.MySQL.DriverPort, KLib.MySQL.Info,
  KLib.Constants,
  System.Classes;

function checkIfMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials): boolean; overload;
function checkIfMysqlVersionIs_v_8(connection: TConnection): boolean; overload;
function getMySQLVersion(mySQLCredentials: TMySQLCredentials): TMySQLVersion; overload;
function getMySQLVersion(connection: TConnection): TMySQLVersion; overload;
function getMySQLVersionAsString(mySQLCredentials: TMySQLCredentials): string; overload;
function getMySQLVersionAsString(connection: TConnection): string; overload;
function getNonStandardsDatabasesAsStringList(mySQLCredentials: TMySQLCredentials): TStringList; overload;
function getNonStandardsDatabasesAsStringList(connection: TConnection): TStringList; overload;
function getMySQLDataDir(mySQLCredentials: TMySQLCredentials): string; overload;
function getMySQLDataDir(connection: TConnection): string; overload;
function getFirstFieldStringListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): TStringList; overload;
function getFirstFieldStringListFromSQLStatement(sqlStatement: string; connection: TConnection): TStringList; overload;
function getFirstFieldListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant; overload;
function getFirstFieldListFromSQLStatement(sqlStatement: string; connection: TConnection): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; connection: TConnection): Variant; overload;

procedure emptyTable(tableName: string; connection: TConnection);

procedure flushPrivileges(mysqlCredentials: TMySQLCredentials); overload;
procedure flushPrivileges(connection: TConnection); overload;

procedure executeScript(sqlStatement: string; mysqlCredentials: TMySQLCredentials); overload;
procedure executeScript(scriptSQL: string; connection: TConnection); overload;
procedure executeQuery(sqlStatement: string; mysqlCredentials: TMySQLCredentials); overload;
procedure executeQuery(sqlStatement: string; connection: TConnection); overload;

function getSQLStatementWithFieldInserted(sqlStatement: string; fieldStmt: string): string;
function getSQLStatementWithJoinStmtInsertedIfNotExists(sqlStatement: string; joinFieldStmt: string): string;
function getSQLStatementWithJoinStmtInserted(sqlStatement: string; joinFieldStmt: string): string;
function getSQLStatementWithWhereStmtInserted(sqlStatement: string; whereFieldStmt: string): string;
function getSQLStatementFromTQuery(query: TQuery; paramsFulfilled: boolean = false): string;

function checkMySQLCredentials(mySQLCredentials: TMySQLCredentials): boolean;
function checkRequiredMySQLProperties(mySQLCredentials: TMySQLCredentials): boolean;

procedure MyISAMToInnoDBInDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);
procedure remove_NO_AUTO_CREATE_USER_inDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);

procedure cleanDataDir_v5_7(pathDataDir: string);
procedure cleanDataDir_v8(pathDataDir: string);

implementation

uses
  KLib.Validate, KLib.MyString, KLib.Utils,
  Data.DB,
  System.SysUtils, System.StrUtils, System.Variants;

function checkIfMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials): boolean;
var
  _result: boolean;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;

    _result := checkIfMysqlVersionIs_v_8(_connection);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := _result;
end;

function checkIfMysqlVersionIs_v_8(connection: TConnection): boolean;
var
  _result: boolean;

  _version: TMySQLVersion;
begin
  _version := getMySQLVersion(connection);
  _result := _version = TMySQLVersion.v_8;

  Result := _result;
end;

function getMySQLVersion(mySQLCredentials: TMySQLCredentials): TMySQLVersion;
var
  version: TMySQLVersion;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;

    version := getMySQLVersion(_connection);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := version;
end;

function getMySQLVersion(connection: TConnection): TMySQLVersion;
const
  MYSQL_V5_5 = '5.5';
  MYSQL_V5_7 = '5.7';
  MYSQL_V8 = '8';

  ERR_MSG = 'Unknown version of MySQL.';
var
  version: TMySQLVersion;

  _versionAsString: string;
begin
  _versionAsString := getMySQLVersionAsString(connection);
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

  Result := version;
end;

function getMySQLVersionAsString(mySQLCredentials: TMySQLCredentials): string;
var
  version: string;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;

    version := getMySQLVersionAsString(_connection);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := version;
end;

function getMySQLVersionAsString(connection: TConnection): string;
const
  SQL_STATEMENT = 'select @@version';
var
  version: string;
begin
  version := getFirstFieldFromSQLStatement(SQL_STATEMENT, connection);

  Result := version;
end;

function getNonStandardsDatabasesAsStringList(mySQLCredentials: TMySQLCredentials): TStringList;
var
  databases: TStringList;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;

    databases := getNonStandardsDatabasesAsStringList(_connection);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := databases;
end;

function getNonStandardsDatabasesAsStringList(connection: TConnection): TStringList;
const
  SQL_STATEMENT = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ("information_schema", "mysql", "performance_schema", "sys")';
var
  databases: TStringList;
begin
  databases := getFirstFieldStringListFromSQLStatement(SQL_STATEMENT, connection);

  Result := databases;
end;

function getMySQLDataDir(mySQLCredentials: TMySQLCredentials): string;
var
  dataDir: string;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;

    dataDir := getMySQLDataDir(_connection);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := dataDir;
end;

function getMySQLDataDir(connection: TConnection): string;
const
  SQL_STATEMENT = 'select @@datadir';
var
  dataDir: string;
begin
  dataDir := getFirstFieldFromSQLStatement(SQL_STATEMENT, connection);

  Result := dataDir;
end;

function getFirstFieldStringListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): TStringList;
var
  fieldStringList: TStringList;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    fieldStringList := getFirstFieldStringListFromSQLStatement(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;

  Result := fieldStringList;
end;

function getFirstFieldStringListFromSQLStatement(sqlStatement: string; connection: TConnection): TStringList;
var
  fieldStringList: TStringList;

  _fieldList: Variant;
begin
  _fieldList := getFirstFieldListFromSQLStatement(sqlStatement, connection);
  fieldStringList := arrayOfVariantToTStringList(_fieldList);
  Result := fieldStringList;
end;

function getFirstFieldListFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant;
var
  fieldListResult: Variant;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    fieldListResult := getFirstFieldListFromSQLStatement(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;

  Result := fieldListResult;
end;

function getFirstFieldListFromSQLStatement(sqlStatement: string; connection: TConnection): Variant;
var
  fieldListResult: Variant;

  _query: TQuery;
  i: integer;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    _query.open;
    fieldListResult := VarArrayCreate([0, _query.RecordCount - 1], varVariant);
    for i := 0 to _query.RecordCount - 1 do
    begin
      fieldListResult[i] := _query.FieldList.Fields[0].value;
      _query.Next;
    end;
    _query.Close;
  finally
    FreeAndNil(_query);
  end;

  Result := fieldListResult;
end;

function getFirstFieldFromSQLStatement(sqlStatement: string; mysqlCredentials: TMySQLCredentials): Variant;
var
  fieldResult: Variant;

  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    fieldResult := getFirstFieldFromSQLStatement(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;

  Result := fieldResult;
end;

function getFirstFieldFromSQLStatement(sqlStatement: string; connection: TConnection): Variant;
var
  fieldResult: Variant;

  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    _query.open;
    fieldResult := _query.FieldList.Fields[0].value;
    _query.Close;
  finally
    FreeAndNil(_query);
  end;

  Result := fieldResult;
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

procedure flushPrivileges(mysqlCredentials: TMySQLCredentials);
var
  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    flushPrivileges(_connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;
end;

procedure flushPrivileges(connection: TConnection);
const
  QUERY_STMT = 'FLUSH PRIVILEGES;';
begin
  executeQuery(QUERY_STMT, connection);
end;

procedure executeScript(sqlStatement: string; mysqlCredentials: TMySQLCredentials);
var
  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    executeScript(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;
end;

procedure executeScript(scriptSQL: string; connection: TConnection);
const
  DEFAULT_DELIMITER = ';';
  DELIMITER_STMT = 'DELIMITER ';
var
  _delimiterStartPosition: integer;
  _currentQuery: string;
  _delimiter: string;
  _scriptSQL: STRING;

  _exit: boolean;
begin
  _delimiter := DEFAULT_DELIMITER;
  _scriptSQL := scriptSQL;
  _exit := false;

  while not _exit do
  begin
    _delimiterStartPosition := myAnsiPos(_delimiter, _scriptSQL, NOT_CASE_SENSITIVE);
    splitStrings(_scriptSQL, _delimiterStartPosition, Length(_delimiter), _currentQuery, _scriptSQL);

    executeQuery(_currentQuery, connection);

    _scriptSQL := _scriptSQL.TrimLeft;
    if _scriptSQL.StartsWith(DELIMITER_STMT, true) then
    begin
      _scriptSQL := _scriptSQL.Remove(0, Length(DELIMITER_STMT));
      _delimiter := _scriptSQL.Chars[0];

      _scriptSQL := _scriptSQL.Remove(0, 1);
    end;
    _scriptSQL := _scriptSQL.TrimRight;
    if _scriptSQL.Length = 0 then
    begin
      _exit := true;
    end;
  end;
end;

procedure executeQuery(sqlStatement: string; mysqlCredentials: TMySQLCredentials);
var
  _connection: TConnection;
begin
  _connection := getValidMySQLTConnection(mysqlCredentials);
  try
    _connection.Connected := true;
    executeQuery(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;
end;

procedure executeQuery(sqlStatement: string; connection: TConnection);
var
  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    _query.ExecSQL;
  finally
    begin
      FreeAndNil(_query);
    end;
  end;
end;

function getSQLStatementWithFieldInserted(sqlStatement: string; fieldStmt: string): string;
var
  _result: string;

  _lastPos: integer;
  _tempQueryStmt: string;
  _insertedString: string;
begin
  _tempQueryStmt := UpperCase(sqlStatement);
  _lastPos := _tempQueryStmt.LastIndexOf('FROM') - 1;
  _insertedString := ', ' + fieldStmt + ' ';
  _result := getMainStringWithSubStringInserted(sqlStatement, _insertedString, _lastPos);

  Result := _result;
end;

function getSQLStatementWithJoinStmtInsertedIfNotExists(sqlStatement: string; joinFieldStmt: string): string;
var
  _result: string;

  _joinFieldStmtAlreadyExists: boolean;
begin
  _joinFieldStmtAlreadyExists := checkIfMainStringContainsSubStringNoCaseSensitive(sqlStatement, joinFieldStmt);
  if not _joinFieldStmtAlreadyExists then
  begin
    _result := getSQLStatementWithJoinStmtInserted(sqlStatement, joinFieldStmt);
  end
  else
  begin
    _result := sqlStatement;
  end;

  Result := _result;
end;

function getSQLStatementWithJoinStmtInserted(sqlStatement: string; joinFieldStmt: string): string;
var
  _result: string;

  _lastPos: integer;
  _tempQueryStmt: string;
  _insertedString: string;
begin
  _tempQueryStmt := UpperCase(sqlStatement);
  _lastPos := _tempQueryStmt.LastIndexOf('WHERE') - 1;
  if _lastPos = -1 then
  begin
    _lastPos := Length(_tempQueryStmt);
  end;
  _insertedString := ' ' + joinFieldStmt + ' ';
  _result := getMainStringWithSubStringInserted(sqlStatement, _insertedString, _lastPos);

  Result := _result;
end;

function getSQLStatementWithWhereStmtInserted(sqlStatement: string; whereFieldStmt: string): string;
var
  _result: string;

  _lastPos: integer;
  _tempQueryStmt: string;
  _insertedString: string;
begin
  _tempQueryStmt := UpperCase(sqlStatement);
  _lastPos := _tempQueryStmt.LastIndexOf('ORDER') - 1;
  if _lastPos = -1 then
  begin
    _lastPos := _tempQueryStmt.LastIndexOf('LIMIT') - 1;
    if _lastPos = -1 then
    begin
      _lastPos := Length(_tempQueryStmt);
    end;
  end;
  _insertedString := ' ' + whereFieldStmt + ' ';
  _result := getMainStringWithSubStringInserted(sqlStatement, _insertedString, _lastPos);

  Result := _result;
end;

function getSQLStatementFromTQuery(query: TQuery; paramsFulfilled: boolean = false): string;
var
  sqlText: myString;

  i: integer;
  _paramName: string;
  _paramValue: Variant;
begin
  sqlText := query.SQL.Text;
  if paramsFulfilled then
  begin
    for i := 0 to query.Params.Count - 1 do
    begin
      _paramName := query.Params[i].Name;
      _paramValue := query.Params[i].Value;

      case query.Params[i].DataType of
        ftUnknown:
          ;
        ftString:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftSmallint:
          sqlText.setParamAsFloat(_paramName, _paramValue, MYSQL_DECIMAL_SEPARATOR);
        ftInteger:
          sqlText.setParamAsFloat(_paramName, _paramValue, MYSQL_DECIMAL_SEPARATOR);
        ftWord:
          ;
        ftBoolean:
          ;
        ftFloat:
          sqlText.setParamAsFloat(_paramName, _paramValue, MYSQL_DECIMAL_SEPARATOR);
        ftCurrency:
          ;
        ftBCD:
          ;
        ftDate:
          sqlText.setParamAsDoubleQuotedDate(_paramName, _paramValue);
        ftTime:
          sqlText.setParamAsDoubleQuotedDateTime(_paramName, _paramValue);
        ftDateTime:
          sqlText.setParamAsDoubleQuotedDateTime(_paramName, _paramValue);
        ftBytes:
          ;
        ftVarBytes:
          ;
        ftAutoInc:
          ;
        ftBlob:
          ;
        ftMemo:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftGraphic:
          ;
        ftFmtMemo:
          ;
        ftParadoxOle:
          ;
        ftDBaseOle:
          ;
        ftTypedBinary:
          ;
        ftCursor:
          ;
        ftFixedChar:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftWideString:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftLargeint:
          ;
        ftADT:
          ;
        ftArray:
          ;
        ftReference:
          ;
        ftDataSet:
          ;
        ftOraBlob:
          ;
        ftOraClob:
          ;
        ftVariant:
          ;
        ftInterface:
          ;
        ftIDispatch:
          ;
        ftGuid:
          ;
        ftTimeStamp:
          ;
        ftFMTBcd:
          ;
        ftFixedWideChar:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftWideMemo:
          sqlText.setParamAsDoubleQuotedString(_paramName, _paramValue);
        ftOraTimeStamp:
          ;
        ftOraInterval:
          ;
        ftLongWord:
          ;
        ftShortint:
          ;
        ftByte:
          ;
        ftExtended:
          ;
        ftConnection:
          ;
        ftParams:
          ;
        ftStream:
          ;
        ftTimeStampOffset:
          ;
        ftObject:
          ;
        ftSingle:
          ;
      end;
    end;
  end;

  Result := sqlText;
end;

function checkMySQLCredentials(mySQLCredentials: TMySQLCredentials): boolean;
var
  _result: boolean;

  _connection: TConnection;
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
    _result := (server <> EMPTY_STRING) and (credentials.username <> EMPTY_STRING)
      and (credentials.password <> EMPTY_STRING) and (port <> 0);
  end;

  Result := _result;
end;

procedure MyISAMToInnoDBInDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);
begin
  replaceTextInFile('ENGINE=MyISAM', 'ENGINE=InnoDB', filename, filenameOutput);
end;

procedure remove_NO_AUTO_CREATE_USER_inDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);
begin
  replaceTextInFile('NO_AUTO_CREATE_USER', EMPTY_STRING, filename, filenameOutput);
end;

procedure cleanDataDir_v5_7(pathDataDir: string);
const
  LIST_FILES_TO_KEEP: array [1 .. 1] of string = (
    'ibdata1'
    );
begin
  deleteFilesInDir(pathDataDir, LIST_FILES_TO_KEEP);
end;

procedure cleanDataDir_v8(pathDataDir: string);
const
  LIST_FILES_TO_KEEP: array [1 .. 3] of string = (
    'ib_buffer_pool',
    'ibdata1',
    'mysql.ibd'
    );
var
  _innodb_temp_path: string;
  _innodb_redo_path: string;
begin
  deleteFilesInDir(pathDataDir, LIST_FILES_TO_KEEP);
  _innodb_temp_path := getCombinedPath(pathDataDir, '#innodb_temp');
  deleteFileIfExists(_innodb_temp_path);
  _innodb_redo_path := getCombinedPath(pathDataDir, '#innodb_redo');
  createDirIfNotExists(_innodb_redo_path);
end;

end.
