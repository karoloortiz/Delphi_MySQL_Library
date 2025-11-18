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

unit KLib.MySQL.Utils;

interface

uses
  KLib.Constants, KLib.Types,
  KLib.MySQL.Driver, KLib.MySQL.Info, KLib.MySQL.Credentials,
  System.Classes;

function checkIfMysqlVersionIs_v_8(connectionString: string): boolean; overload;
function checkIfMysqlVersionIs_v_8(credentials: KLib.MySQL.Credentials.TCredentials): boolean; overload;
function checkIfMysqlVersionIs_v_8(connection: TConnection): boolean; overload;
function getMySQLVersion(connectionString: string): TMySQLVersion; overload;
function getMySQLVersion(credentials: KLib.MySQL.Credentials.TCredentials): TMySQLVersion; overload;
function getMySQLVersion(connection: TConnection): TMySQLVersion; overload;
function getMySQLVersionAsString(connectionString: string): string; overload;
function getMySQLVersionAsString(credentials: KLib.MySQL.Credentials.TCredentials): string; overload;
function getMySQLVersionAsString(connection: TConnection): string; overload;
function getNonStandardsDatabasesAsStringList(connectionString: string): TStringList; overload;
function getNonStandardsDatabasesAsStringList(credentials: KLib.MySQL.Credentials.TCredentials): TStringList; overload;
function getNonStandardsDatabasesAsStringList(connection: TConnection): TStringList; overload;
function getMySQLDataDir(connectionString: string): string; overload;
function getMySQLDataDir(credentials: KLib.MySQL.Credentials.TCredentials): string; overload;
function getMySQLDataDir(connection: TConnection): string; overload;
function getFirstFieldStringListFromSQLStatement(sqlStatement: string; connectionString: string): TStringList; overload;
function getFirstFieldStringListFromSQLStatement(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials): TStringList; overload;
function getFirstFieldStringListFromSQLStatement(sqlStatement: string; connection: TConnection): TStringList; overload;
function getFirstFieldListFromSQLStatement(sqlStatement: string; connectionString: string): Variant; overload;
function getFirstFieldListFromSQLStatement(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials): Variant; overload;
function getFirstFieldListFromSQLStatement(sqlStatement: string; connection: TConnection): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; connectionString: string): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials): Variant; overload;
function getFirstFieldFromSQLStatement(sqlStatement: string; connection: TConnection): Variant; overload;

function getRecordCountFromSQLStatement(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials): integer; overload;
function getRecordCountFromSQLStatement(sqlStatement: string; connection: TConnection): integer; overload;

procedure emptyTable(tableName: string; connection: TConnection);

procedure flushPrivileges(connectionString: string); overload;
procedure flushPrivileges(credentials: KLib.MySQL.Credentials.TCredentials); overload;
procedure flushPrivileges(connection: TConnection); overload;

procedure exportCSV(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; fileName: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure exportCSV(sqlStatement: string; connection: TConnection; fileName: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure exportCSV(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; fileName: string; options: TCsvExportOptions; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure exportCSV(sqlStatement: string; connection: TConnection; fileName: string; options: TCsvExportOptions; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;

procedure executeScript(sqlStatement: string; connectionString: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure executeScript(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure executeScript(scriptSQL: string; connection: TConnection; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure executeQuery(sqlStatement: string; connectionString: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure executeQuery(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
procedure executeQuery(sqlStatement: string; connection: TConnection; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;

procedure refreshQueryKeepingPosition(query: TQuery);

function getSQLStatementWithFieldInserted(sqlStatement: string; fieldStmt: string): string;
function getSQLStatementWithJoinStmtInsertedIfNotExists(sqlStatement: string; joinFieldStmt: string): string;
function getSQLStatementWithJoinStmtInserted(sqlStatement: string; joinFieldStmt: string): string;
function getSQLStatementWithWhereStmtInserted(sqlStatement: string; whereFieldStmt: string): string;
function getSQLStatementFromTQuery(query: TQuery; paramsFulfilled: boolean = false): string;

function checkMySQLCredentials(connectionString: string): boolean; overload;
function checkMySQLCredentials(credentials: KLib.MySQL.Credentials.TCredentials): boolean; overload;
function checkRequiredMySQLProperties(credentials: KLib.MySQL.Credentials.TCredentials): boolean;

function parseConnectionStringToCredentials(connectionString: string): TCredentials;
function parseJDBCConnectionString(connectionString: string): TCredentials;

procedure MyISAMToInnoDBInDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);
procedure remove_NO_AUTO_CREATE_USER_inDumpFile(filename: string; filenameOutput: string = EMPTY_STRING);

procedure cleanDataDir_v5_7(pathDataDir: string);
procedure cleanDataDir_v8(pathDataDir: string);

type
  TDumpOptions = record
    includeData: Boolean;
    includeStructure: Boolean;
    includeDrop: Boolean;
    includeIndexes: Boolean;
    includeConstraints: Boolean;
    includeTriggers: Boolean;
    batchSize: Integer;
    extendedInsert: Boolean;
    completeInsert: Boolean;
    disableKeys: Boolean;
    whereClause: string;
    lockTables: Boolean;

    procedure clear;
  end;

function dumpTable(connectionString: string; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string; overload;
function dumpTable(connection: TConnection; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string; overload;
function dumpTable(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string; overload;
function dumpTable(connectionString: string; tableName: string; databaseName: string = EMPTY_STRING): string; overload;
function dumpTable(connection: TConnection; tableName: string; databaseName: string = EMPTY_STRING): string; overload;
function dumpTable(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; databaseName: string = EMPTY_STRING): string; overload;
procedure dumpTableToFile(connectionString: string; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToFile(connection: TConnection; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToFile(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToFile(connectionString: string; tableName: string; filename: string; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToFile(connection: TConnection; tableName: string; filename: string; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToFile(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; filename: string; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToStream(connection: TConnection; tableName: string; stream: TStream; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpTableToStream(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; stream: TStream; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpDatabaseToFile(connection: TConnection; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;
procedure dumpDatabaseToFile(credentials: KLib.MySQL.Credentials.TCredentials; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING); overload;

implementation

uses
  System.SysUtils, System.StrUtils, System.Variants,
  Data.DB,
{$ifdef KLIB_MYSQL_FIREDAC}
  FireDAC.Stan.Param,
{$ifend}
  KLib.Validate, KLib.sqlstring, KLib.Utils, KLib.StringUtils, KLib.Csv,
  KLib.FileSystem;

function checkIfMysqlVersionIs_v_8(credentials: KLib.MySQL.Credentials.TCredentials): boolean;
var
  _result: boolean;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getMySQLVersion(credentials: KLib.MySQL.Credentials.TCredentials): TMySQLVersion;
var
  version: TMySQLVersion;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getMySQLVersionAsString(credentials: KLib.MySQL.Credentials.TCredentials): string;
var
  version: string;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getNonStandardsDatabasesAsStringList(credentials: KLib.MySQL.Credentials.TCredentials): TStringList;
var
  databases: TStringList;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getMySQLDataDir(credentials: KLib.MySQL.Credentials.TCredentials): string;
var
  dataDir: string;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getFirstFieldStringListFromSQLStatement(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials): TStringList;
var
  fieldStringList: TStringList;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getFirstFieldListFromSQLStatement(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials): Variant;
var
  fieldListResult: Variant;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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
    try
      _query.open;
      fieldListResult := VarArrayCreate([0, _query.RecordCount - 1], varVariant);
      for i := 0 to _query.RecordCount - 1 do
      begin
        fieldListResult[i] := _query.FieldList.Fields[0].value;
        _query.Next;
      end;
      _query.Close;
    except
      on E: Exception do
      begin
        raise Exception.Create('Error Klib.MySQL: ' + E.Message);
      end;
    end;
  finally
    FreeAndNil(_query);
  end;

  Result := fieldListResult;
end;

function getFirstFieldFromSQLStatement(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials): Variant;
var
  fieldResult: Variant;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

function getRecordCountFromSQLStatement(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials): integer;
var
  recordCount: integer;

  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    recordCount := getRecordCountFromSQLStatement(sqlStatement, _connection);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;

  Result := recordCount;
end;

function getRecordCountFromSQLStatement(sqlStatement: string; connection: TConnection): integer;
var
  recordCount: integer;

  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    _query.open;
    recordCount := _query.recordCount;
    _query.Close;
  finally
    FreeAndNil(_query);
  end;

  Result := recordCount;
end;

procedure emptyTable(tableName: string; connection: TConnection);
const
  PARAM_TABLENAME = ':TABLENAME';
  DELETE_FROM_WHERE_PARAM_TABLENAME =
    'DELETE' + sLineBreak +
    'FROM' + sLineBreak +
    PARAM_TABLENAME;
var
  _queryStmt: sqlstring;
begin
  _queryStmt := DELETE_FROM_WHERE_PARAM_TABLENAME;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);
  executeQuery(_queryStmt, connection);
end;

procedure flushPrivileges(credentials: KLib.MySQL.Credentials.TCredentials);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
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

procedure exportCSV(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; fileName: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
var
  _csvExportoptions: TCsvExportOptions;
begin
  _csvExportoptions := TCsvExportOptions.getDefault;
  exportCSV(sqlStatement, credentials, fileName, _csvExportoptions, isRaiseExceptionEnabled);
end;

procedure exportCSV(sqlStatement: string; connection: TConnection; fileName: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
var
  _csvExportoptions: TCsvExportOptions;
begin
  _csvExportoptions := TCsvExportOptions.getDefault;
  exportCSV(sqlStatement, connection, fileName, _csvExportoptions, isRaiseExceptionEnabled);
end;

procedure exportCSV(sqlStatement: string; credentials: KLib.MySQL.Credentials.TCredentials; fileName: string; options: TCsvExportOptions; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
var
  _query: TQuery;
begin
  _query := getTQuery(credentials, sqlStatement);
  try
    try
      _query.Open;
      exportDatasetToCSV(_query, fileName, options);
      _query.Close;
    except
      on E: Exception do
      begin
        if isRaiseExceptionEnabled then
        begin
          raise Exception.Create('Open query: ' + e.Message);
        end;
      end;
    end;
  finally
    begin
      FreeAndNil(_query);
    end;
  end;
end;

procedure exportCSV(sqlStatement: string; connection: TConnection; fileName: string; options: TCsvExportOptions; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION); overload;
var
  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    try
      _query.Open;
      exportDatasetToCSV(_query, fileName, options);
      _query.Close;
    except
      on E: Exception do
      begin
        if isRaiseExceptionEnabled then
        begin
          raise Exception.Create('Open query: ' + e.Message);
        end;
      end;
    end;
  finally
    begin
      FreeAndNil(_query);
    end;
  end;
end;

procedure executeScript(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    executeScript(sqlStatement, _connection, isRaiseExceptionEnabled);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;
end;

procedure executeScript(scriptSQL: string; connection: TConnection; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
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

    executeQuery(_currentQuery, connection, isRaiseExceptionEnabled);

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

procedure executeQuery(sqlStatement: string;
  credentials: KLib.MySQL.Credentials.TCredentials; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    executeQuery(sqlStatement, _connection, isRaiseExceptionEnabled);
    _connection.Connected := false;
  finally
    FreeAndNil(_connection);
  end;
end;

procedure executeQuery(sqlStatement: string; connection: TConnection; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
var
  _query: TQuery;
begin
  _query := getTQuery(connection, sqlStatement);
  try
    try
      _query.ExecSQL;
    except
      on E: Exception do
      begin
        if isRaiseExceptionEnabled then
        begin
          raise Exception.Create('Execute query: ' + e.Message);
        end;
      end;
    end;
  finally
    begin
      FreeAndNil(_query);
    end;
  end;
end;

procedure refreshQueryKeepingPosition(query: TQuery);
var
  _bookmark: TBookmark;
begin
  _bookmark := Query.GetBookmark;
  query.Close;
  query.Open;
  query.GotoBookmark(_bookmark);
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
  _joinFieldStmtAlreadyExists := checkIfStringContainsSubStringNoCaseSensitive(sqlStatement, joinFieldStmt);
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
  sqlText: sqlstring;

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
          sqlText.paramByNameAsString(_paramName, string(_paramValue), false);
        ftSmallint:
          sqlText.paramByNameAsInteger(_paramName, _paramValue);
        ftInteger:
          sqlText.paramByNameAsInteger(_paramName, _paramValue);
        ftWord:
          ;
        ftBoolean:
          ;
        ftFloat:
          sqlText.paramByNameAsFloat(_paramName, _paramValue, MYSQL_DECIMAL_SEPARATOR);
        ftCurrency:
          ;
        ftBCD:
          ;
        ftDate:
          sqlText.paramByNameAsDate(_paramName, _paramValue);
        ftTime:
          sqlText.paramByNameAsDateTime(_paramName, _paramValue);
        ftDateTime:
          sqlText.paramByNameAsDateTime(_paramName, _paramValue);
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

function checkMySQLCredentials(credentials: KLib.MySQL.Credentials.TCredentials): boolean;
var
  _result: boolean;

  _connection: TConnection;
begin
  _connection := getTConnection(credentials);
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

function checkRequiredMySQLProperties(credentials: KLib.MySQL.Credentials.TCredentials): boolean;
var
  _result: boolean;
begin
  with credentials do
  begin
    _result := (server <> EMPTY_STRING) and (credentials.username <> EMPTY_STRING)
      and (credentials.password <> EMPTY_STRING) and (port <> 0);
  end;

  Result := _result;
end;

function parseConnectionStringToCredentials(connectionString: string): TCredentials;
var
  _result: TCredentials;
  _pairs: TStringList;
  _pair: TStringList;
  _key, _value: string;
  _connectionString: string;
  i: integer;
begin
  _result.setDefault;
  _connectionString := connectionString;

  // Check if it's a JDBC-style connection string
  if _connectionString.StartsWith('jdbc:mysql://', true) or
    _connectionString.StartsWith('mysql://', true) then
  begin
    Result := parseJDBCConnectionString(_connectionString);
    Exit;
  end;

  _pairs := TStringList.Create;
  _pair := TStringList.Create;
  try
    _pairs.Delimiter := ';';
    _pairs.StrictDelimiter := true;
    _pairs.DelimitedText := _connectionString;

    for i := 0 to _pairs.Count - 1 do
    begin
      _pair.Clear;
      _pair.Delimiter := '=';
      _pair.StrictDelimiter := true;
      _pair.DelimitedText := _pairs[i];

      if _pair.Count = 2 then
      begin
        _key := LowerCase(Trim(_pair[0]));
        _value := Trim(_pair[1]);

        // Remove quotes if present
        if (_value.Length > 1) and
          ((_value.StartsWith('"') and _value.EndsWith('"')) or
          (_value.StartsWith('''') and _value.EndsWith(''''))) then
        begin
          _value := _value.Substring(1, _value.Length - 2);
        end;

        if (_key = 'server') or (_key = 'host') or (_key = 'data source') or (_key = 'datasource') then
          _result.server := _value
        else if (_key = 'port') then
          _result.port := StrToIntDef(_value, 3306)
        else if (_key = 'database') or (_key = 'initial catalog') or (_key = 'initialcatalog') then
          _result.database := _value
        else if (_key = 'user id') or (_key = 'userid') or (_key = 'uid') or (_key = 'user') or (_key = 'username') then
          _result.credentials.username := _value
        else if (_key = 'password') or (_key = 'pwd') then
          _result.credentials.password := _value
        else if (_key = 'ssl mode') or (_key = 'sslmode') then
          _result.useSSL := (LowerCase(_value) = 'required') or (LowerCase(_value) = 'preferred') or (LowerCase(_value) = 'true')
        else if (_key = 'allow user variables') or (_key = 'allowuservariables') then
          // Ignore - compatibility parameter
        else if (_key = 'charset') or (_key = 'character set') or (_key = 'characterset') then
          // Ignore - compatibility parameter
        else if (_key = 'connect timeout') or (_key = 'connecttimeout') or (_key = 'connection timeout') or (_key = 'connectiontimeout') then
          // Ignore - compatibility parameter
        else if (_key = 'default command timeout') or (_key = 'defaultcommandtimeout') then
          // Ignore - compatibility parameter
        else if (_key = 'pooling') then
          // Ignore - compatibility parameter
        else if (_key = 'min pool size') or (_key = 'minpoolsize') then
          // Ignore - compatibility parameter
        else if (_key = 'max pool size') or (_key = 'maxpoolsize') then
          // Ignore - compatibility parameter
        else if (_key = 'use_caching_sha2_password_dll') then
          _result.use_caching_sha2_password_dll := (LowerCase(_value) = 'true') or (_value = '1');
      end;
    end;
  finally
    FreeAndNil(_pairs);
    FreeAndNil(_pair);
  end;

  Result := _result;
end;

function checkMySQLCredentials(connectionString: string): boolean;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := checkMySQLCredentials(_credentials);
end;

procedure flushPrivileges(connectionString: string);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  flushPrivileges(_credentials);
end;

procedure executeScript(sqlStatement: string; connectionString: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  executeScript(sqlStatement, _credentials, isRaiseExceptionEnabled);
end;

procedure executeQuery(sqlStatement: string; connectionString: string; isRaiseExceptionEnabled: boolean = RAISE_EXCEPTION);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  executeQuery(sqlStatement, _credentials, isRaiseExceptionEnabled);
end;

function getFirstFieldFromSQLStatement(sqlStatement: string; connectionString: string): Variant;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getFirstFieldFromSQLStatement(sqlStatement, _credentials);
end;

function getFirstFieldListFromSQLStatement(sqlStatement: string; connectionString: string): Variant;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getFirstFieldListFromSQLStatement(sqlStatement, _credentials);
end;

function getFirstFieldStringListFromSQLStatement(sqlStatement: string; connectionString: string): TStringList;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getFirstFieldStringListFromSQLStatement(sqlStatement, _credentials);
end;

function getMySQLDataDir(connectionString: string): string;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getMySQLDataDir(_credentials);
end;

function getNonStandardsDatabasesAsStringList(connectionString: string): TStringList;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getNonStandardsDatabasesAsStringList(_credentials);
end;

function getMySQLVersionAsString(connectionString: string): string;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getMySQLVersionAsString(_credentials);
end;

function getMySQLVersion(connectionString: string): TMySQLVersion;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := getMySQLVersion(_credentials);
end;

function checkIfMysqlVersionIs_v_8(connectionString: string): boolean;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := checkIfMysqlVersionIs_v_8(_credentials);
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

procedure TDumpOptions.clear;
begin
  Self.includeData := true;
  Self.includeStructure := true;
  Self.includeDrop := true;
  Self.includeIndexes := false;
  Self.includeConstraints := false;
  Self.includeTriggers := false;
  Self.batchSize := 1000;
  Self.extendedInsert := true;
  Self.completeInsert := false;
  Self.disableKeys := true;
  Self.whereClause := EMPTY_STRING;
  Self.lockTables := true;
end;

function dumpTable(connection: TConnection; tableName: string; databaseName: string = EMPTY_STRING): string;
var
  _options: TDumpOptions;
begin
  _options.clear;
  Result := dumpTable(connection, tableName, _options, databaseName);
end;

function dumpTable(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; databaseName: string = EMPTY_STRING): string;
var
  _options: TDumpOptions;
begin
  _options.clear;
  Result := dumpTable(credentials, tableName, _options, databaseName);
end;

function dumpTable(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string;
var
  _result: string;
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    _result := dumpTable(_connection, tableName, options, databaseName);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;

  Result := _result;
end;

function dumpTable(connection: TConnection; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string;
const
  SQL_SHOW_CREATE_TABLE = 'SHOW CREATE TABLE :database.:table';
  SQL_SELECT_DATA = 'SELECT * FROM :database.:table';
  SQL_SHOW_INDEXES = 'SHOW INDEX FROM :database.:table WHERE Key_name != "PRIMARY"';
  SQL_SHOW_CONSTRAINTS = 'SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table AND REFERENCED_TABLE_NAME IS NOT NULL';
  SQL_SHOW_TRIGGERS = 'SHOW TRIGGERS FROM :database WHERE `Table` = :table';
var
  _result: string;
  _sqlStmt: sqlstring;
  _query: TQuery;
  _dumpLines: TStringList;
  _insertStatement: string;
  _fieldsCount: integer;
  _fieldNames: TStringList;
  i: integer;
  _fieldValue: string;
  _timestamp: string;
  _batchCount: integer;
  _valuesList: TStringList;
  _actualDatabaseName: string;
  _qualifiedTableName: string;
begin
  _dumpLines := TStringList.Create;
  _fieldNames := TStringList.Create;
  _valuesList := TStringList.Create;
  try
    if databaseName = EMPTY_STRING then
    begin
      _actualDatabaseName := connection.Database;
      _qualifiedTableName := '`' + tableName + '`';
    end
    else
    begin
      _actualDatabaseName := databaseName;
      _qualifiedTableName := '`' + databaseName + '`.`' + tableName + '`';
    end;

    _timestamp := FormatDateTime('yyyy-mm-dd hh:nn:ss', Now);

    _dumpLines.Add('-- MySQL dump generated on ' + _timestamp);
    if _actualDatabaseName <> EMPTY_STRING then
    begin
      _dumpLines.Add('-- Database: ' + _actualDatabaseName);
    end;
    _dumpLines.Add('-- Table: ' + tableName);
    _dumpLines.Add('');

    if options.includeStructure then
    begin
      if options.includeDrop then
      begin
        _dumpLines.Add('DROP TABLE IF EXISTS ' + _qualifiedTableName + ';');
        _dumpLines.Add('');
      end;

      _sqlStmt := SQL_SHOW_CREATE_TABLE;
      _sqlStmt.setParamAsString('database', _actualDatabaseName);
      _sqlStmt.setParamAsString('table', tableName);

      _query := getTQuery(connection, _sqlStmt);
      try
        _query.Open;
        if not _query.IsEmpty then
        begin
          _dumpLines.Add('-- Table structure for ' + tableName);

          _insertStatement := _query.Fields[1].AsString;
          if databaseName <> EMPTY_STRING then
          begin
            _insertStatement := StringReplace(_insertStatement, 'CREATE TABLE `' + tableName + '`',
              'CREATE TABLE ' + _qualifiedTableName, [rfIgnoreCase]);
          end;

          _dumpLines.Add(_insertStatement + ';');
          _dumpLines.Add('');
        end;
      finally
        FreeAndNil(_query);
      end;
    end;

    if options.includeData then
    begin
      _sqlStmt := SQL_SELECT_DATA;
      _sqlStmt.setParamAsString('database', _actualDatabaseName);
      _sqlStmt.setParamAsString('table', tableName);

      if options.whereClause <> EMPTY_STRING then
      begin
        _sqlStmt := _sqlStmt + ' WHERE ' + options.whereClause;
      end;

      _query := getTQuery(connection, _sqlStmt);
      try
        _query.Open;
        if not _query.IsEmpty then
        begin
          _dumpLines.Add('-- Data for table ' + tableName);

          if options.lockTables then
          begin
            _dumpLines.Add('LOCK TABLES ' + _qualifiedTableName + ' WRITE;');
          end;

          if options.disableKeys then
          begin
            _dumpLines.Add('ALTER TABLE ' + _qualifiedTableName + ' DISABLE KEYS;');
          end;

          _fieldsCount := _query.FieldCount;

          if options.completeInsert then
          begin
            for i := 0 to _fieldsCount - 1 do
            begin
              _fieldNames.Add('`' + _query.Fields[i].FieldName + '`');
            end;
          end;

          _batchCount := 0;
          _valuesList.Clear;

          _query.First;
          while not _query.Eof do
          begin
            _insertStatement := '(';

            for i := 0 to _fieldsCount - 1 do
            begin
              if _query.Fields[i].IsNull then
              begin
                _fieldValue := 'NULL';
              end
              else
              begin
                case _query.Fields[i].DataType of
                  ftString, ftMemo, ftWideString, ftWideMemo:
                    _fieldValue := QuotedStr(_query.Fields[i].AsString);
                  ftDateTime, ftDate, ftTime:
                    _fieldValue := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', _query.Fields[i].AsDateTime));
                  ftBoolean:
                    _fieldValue := IfThen(_query.Fields[i].AsBoolean, '1', '0');
                  ftFloat, ftCurrency, ftBCD:
                    _fieldValue := StringReplace(_query.Fields[i].AsString, ',', '.', []);
                  ftBlob, ftBytes, ftVarBytes:
                    if _query.Fields[i].IsNull then
                      _fieldValue := 'NULL'
                    else
                      _fieldValue := QuotedStr(_query.Fields[i].AsString);
                else
                  _fieldValue := _query.Fields[i].AsString;
                end;
              end;

              if i < _fieldsCount - 1 then
                _insertStatement := _insertStatement + _fieldValue + ','
              else
                _insertStatement := _insertStatement + _fieldValue;
            end;

            _insertStatement := _insertStatement + ')';

            if options.extendedInsert and (options.batchSize > 1) then
            begin
              _valuesList.Add(_insertStatement);
              _batchCount := _batchCount + 1;

              _query.Next;

              if (_batchCount >= options.batchSize) or _query.Eof then
              begin
                _insertStatement := 'INSERT INTO ' + _qualifiedTableName;
                if options.completeInsert then
                begin
                  _insertStatement := _insertStatement + ' (';
                  for i := 0 to _fieldNames.Count - 1 do
                  begin
                    _insertStatement := _insertStatement + _fieldNames[i];
                    if i < _fieldNames.Count - 1 then
                      _insertStatement := _insertStatement + ',';
                  end;
                  _insertStatement := _insertStatement + ')';
                end;
                _insertStatement := _insertStatement + ' VALUES ';
                for i := 0 to _valuesList.Count - 1 do
                begin
                  _insertStatement := _insertStatement + _valuesList[i];
                  if i < _valuesList.Count - 1 then
                    _insertStatement := _insertStatement + ',';
                end;
                _insertStatement := _insertStatement + ';';
                _dumpLines.Add(_insertStatement);

                _valuesList.Clear;
                _batchCount := 0;
              end;
            end
            else
            begin
              _fieldValue := _insertStatement;
              _insertStatement := 'INSERT INTO ' + _qualifiedTableName;
              if options.completeInsert then
              begin
                _insertStatement := _insertStatement + ' (';
                for i := 0 to _fieldNames.Count - 1 do
                begin
                  _insertStatement := _insertStatement + _fieldNames[i];
                  if i < _fieldNames.Count - 1 then
                    _insertStatement := _insertStatement + ',';
                end;
                _insertStatement := _insertStatement + ')';
              end;
              _insertStatement := _insertStatement + ' VALUES ' + _fieldValue + ';';
              _dumpLines.Add(_insertStatement);
              _query.Next;
            end;
          end;

          if options.disableKeys then
          begin
            _dumpLines.Add('ALTER TABLE ' + _qualifiedTableName + ' ENABLE KEYS;');
          end;

          if options.lockTables then
          begin
            _dumpLines.Add('UNLOCK TABLES;');
          end;

          _dumpLines.Add('');
        end;
      finally
        FreeAndNil(_query);
      end;
    end;

    if options.includeIndexes then
    begin
      _sqlStmt := SQL_SHOW_INDEXES;
      _sqlStmt.setParamAsString('database', _actualDatabaseName);
      _sqlStmt.setParamAsString('table', tableName);

      _query := getTQuery(connection, _sqlStmt);
      try
        _query.Open;
        if not _query.IsEmpty then
        begin
          _dumpLines.Add('-- Indexes for table ' + tableName);
          _query.First;
          while not _query.Eof do
          begin
            _dumpLines.Add('CREATE INDEX `' + _query.FieldByName('Key_name').AsString + '` ON `' +
              tableName + '` (`' + _query.FieldByName('Column_name').AsString + '`);');
            _query.Next;
          end;
          _dumpLines.Add('');
        end;
      finally
        FreeAndNil(_query);
      end;
    end;

    if options.includeConstraints then
    begin
      _sqlStmt := SQL_SHOW_CONSTRAINTS;
      _sqlStmt.paramByNameAsString('schema', _actualDatabaseName);
      _sqlStmt.paramByNameAsString('table', tableName);

      _query := getTQuery(connection, _sqlStmt);
      try
        _query.Open;
        if not _query.IsEmpty then
        begin
          _dumpLines.Add('-- Constraints for table ' + tableName);
          _query.First;
          while not _query.Eof do
          begin
            _dumpLines.Add('ALTER TABLE `' + _query.FieldByName('TABLE_NAME').AsString +
              '` ADD CONSTRAINT `' + _query.FieldByName('CONSTRAINT_NAME').AsString +
              '` FOREIGN KEY (`' + _query.FieldByName('COLUMN_NAME').AsString +
              '`) REFERENCES `' + _query.FieldByName('REFERENCED_TABLE_NAME').AsString +
              '` (`' + _query.FieldByName('REFERENCED_COLUMN_NAME').AsString + '`);');
            _query.Next;
          end;
          _dumpLines.Add('');
        end;
      finally
        FreeAndNil(_query);
      end;
    end;

    if options.includeTriggers then
    begin
      _sqlStmt := SQL_SHOW_TRIGGERS;
      _sqlStmt.setParamAsString('database', _actualDatabaseName);
      _sqlStmt.setParamAsString('table', tableName);

      _query := getTQuery(connection, _sqlStmt);
      try
        _query.Open;
        if not _query.IsEmpty then
        begin
          _dumpLines.Add('-- Triggers for table ' + tableName);
          _query.First;
          while not _query.Eof do
          begin
            _dumpLines.Add('DELIMITER $$');
            _dumpLines.Add('CREATE TRIGGER `' + _query.FieldByName('Trigger').AsString + '` ' +
              _query.FieldByName('Timing').AsString + ' ' + _query.FieldByName('Event').AsString +
              ' ON `' + tableName + '` FOR EACH ROW BEGIN ' + _query.FieldByName('Statement').AsString + ' END$$');
            _dumpLines.Add('DELIMITER ;');
            _query.Next;
          end;
          _dumpLines.Add('');
        end;
      finally
        FreeAndNil(_query);
      end;
    end;

    _result := _dumpLines.Text;
  finally
    FreeAndNil(_dumpLines);
    FreeAndNil(_fieldNames);
    FreeAndNil(_valuesList);
  end;

  Result := _result;
end;

function dumpTable(connectionString: string; tableName: string; options: TDumpOptions; databaseName: string = EMPTY_STRING): string;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := dumpTable(_credentials, tableName, options, databaseName);
end;

function dumpTable(connectionString: string; tableName: string; databaseName: string = EMPTY_STRING): string;
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  Result := dumpTable(_credentials, tableName, databaseName);
end;

procedure dumpTableToFile(connection: TConnection; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _dumpContent: string;
begin
  _dumpContent := dumpTable(connection, tableName, options, databaseName);
  saveToFile(_dumpContent, filename);
end;

procedure dumpTableToFile(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    dumpTableToFile(_connection, tableName, filename, options, databaseName);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;
end;

procedure dumpTableToFile(connection: TConnection; tableName: string; filename: string; databaseName: string = EMPTY_STRING);
var
  _options: TDumpOptions;
begin
  _options.clear;
  dumpTableToFile(connection, tableName, filename, _options, databaseName);
end;

procedure dumpTableToFile(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; filename: string; databaseName: string = EMPTY_STRING);
var
  _options: TDumpOptions;
begin
  _options.clear;
  dumpTableToFile(credentials, tableName, filename, _options, databaseName);
end;

procedure dumpTableToFile(connectionString: string; tableName: string; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  dumpTableToFile(_credentials, tableName, filename, options, databaseName);
end;

procedure dumpTableToFile(connectionString: string; tableName: string; filename: string; databaseName: string = EMPTY_STRING);
var
  _credentials: TCredentials;
begin
  _credentials := parseConnectionStringToCredentials(connectionString);
  dumpTableToFile(_credentials, tableName, filename, databaseName);
end;

procedure dumpTableToStream(connection: TConnection; tableName: string; stream: TStream; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _dumpContent: string;
  _stringStream: TStringStream;
begin
  _dumpContent := dumpTable(connection, tableName, options, databaseName);
  _stringStream := TStringStream.Create(_dumpContent, TEncoding.UTF8);
  try
    stream.CopyFrom(_stringStream, 0);
  finally
    FreeAndNil(_stringStream);
  end;
end;

procedure dumpTableToStream(credentials: KLib.MySQL.Credentials.TCredentials; tableName: string; stream: TStream; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    dumpTableToStream(_connection, tableName, stream, options, databaseName);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;
end;

procedure dumpDatabaseToFile(connection: TConnection; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING);
const
  SQL_SHOW_TABLES = 'SHOW TABLES FROM :database';
var
  _sqlStmt: sqlstring;
  _query: TQuery;
  _allDumpContent: string;
  _tableDumpContent: string;
  _actualDatabaseName: string;
begin
  if databaseName = EMPTY_STRING then
  begin
    _actualDatabaseName := connection.Database;
  end
  else
  begin
    _actualDatabaseName := databaseName;
  end;

  _allDumpContent := '-- Full database dump for: ' + _actualDatabaseName + sLineBreak;
  _allDumpContent := _allDumpContent + '-- Generated on: ' + FormatDateTime('yyyy-mm-dd hh:nn:ss', Now) + sLineBreak + sLineBreak;

  _sqlStmt := SQL_SHOW_TABLES;
  _sqlStmt.setParamAsString('database', _actualDatabaseName);

  _query := getTQuery(connection, _sqlStmt);
  try
    _query.Open;
    if not _query.IsEmpty then
    begin
      _query.First;
      while not _query.Eof do
      begin
        _tableDumpContent := dumpTable(connection, _query.Fields[0].AsString, options, databaseName);
        _allDumpContent := _allDumpContent + _tableDumpContent + sLineBreak;
        _query.Next;
      end;
    end;
  finally
    FreeAndNil(_query);
  end;

  saveToFile(_allDumpContent, filename);
end;

procedure dumpDatabaseToFile(credentials: KLib.MySQL.Credentials.TCredentials; filename: string; options: TDumpOptions; databaseName: string = EMPTY_STRING);
var
  _connection: TConnection;
begin
  _connection := getValidTConnection(credentials);
  try
    _connection.Connected := true;
    dumpDatabaseToFile(_connection, filename, options, databaseName);
  finally
    _connection.Connected := false;
    FreeAndNil(_connection);
  end;
end;

function parseJDBCConnectionString(connectionString: string): TCredentials;
var
  _result: TCredentials;
  _url: string;
  _queryParams: string;
  _hostPort: string;
  _database: string;
  _pairs: TStringList;
  _pair: TStringList;
  _key, _value: string;
  _colonPos: integer;
  _questionPos: integer;
  _slashPos: integer;
  i: integer;
begin
  _result.setDefault;
  _url := connectionString;

  // Remove jdbc: prefix if present
  if _url.StartsWith('jdbc:', true) then
    _url := _url.Substring(5);

  // Remove mysql:// prefix
  if _url.StartsWith('mysql://', true) then
    _url := _url.Substring(8)
  else if _url.StartsWith('//', true) then
    _url := _url.Substring(2);

  // Split URL and query parameters
  _questionPos := _url.IndexOf('?');
  if _questionPos > 0 then
  begin
    _queryParams := _url.Substring(_questionPos + 1);
    _url := _url.Substring(0, _questionPos);
  end
  else
    _queryParams := '';

  // Parse host:port/database
  _slashPos := _url.IndexOf('/');
  if _slashPos > 0 then
  begin
    _hostPort := _url.Substring(0, _slashPos);
    _database := _url.Substring(_slashPos + 1);
    _result.database := _database;
  end
  else
    _hostPort := _url;

  // Parse host and port
  _colonPos := _hostPort.LastIndexOf(':');
  if _colonPos > 0 then
  begin
    _result.server := _hostPort.Substring(0, _colonPos);
    _result.port := StrToIntDef(_hostPort.Substring(_colonPos + 1), 3306);
  end
  else
  begin
    _result.server := _hostPort;
    _result.port := 3306;
  end;

  // Parse query parameters
  if _queryParams <> '' then
  begin
    _pairs := TStringList.Create;
    _pair := TStringList.Create;
    try
      _pairs.Delimiter := '&';
      _pairs.StrictDelimiter := true;
      _pairs.DelimitedText := _queryParams;

      for i := 0 to _pairs.Count - 1 do
      begin
        _pair.Clear;
        _pair.Delimiter := '=';
        _pair.StrictDelimiter := true;
        _pair.DelimitedText := _pairs[i];

        if _pair.Count = 2 then
        begin
          _key := LowerCase(Trim(_pair[0]));
          _value := Trim(_pair[1]);

          if (_key = 'user') or (_key = 'username') then
            _result.credentials.username := _value
          else if (_key = 'password') or (_key = 'passwd') then
            _result.credentials.password := _value
          else if (_key = 'usessl') or (_key = 'ssl') then
            _result.useSSL := (LowerCase(_value) = 'true') or (_value = '1')
          else if (_key = 'use_caching_sha2_password_dll') then
            _result.use_caching_sha2_password_dll := (LowerCase(_value) = 'true') or (_value = '1');
        end;
      end;
    finally
      FreeAndNil(_pairs);
      FreeAndNil(_pair);
    end;
  end;

  Result := _result;
end;

end.
