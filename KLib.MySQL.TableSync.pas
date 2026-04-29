{
  KLib Version = 4.0
  The Clear BSD License

  Copyright (c) 2026 by Karol De Nery Ortiz LLave. All rights reserved.
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

unit KLib.MySQL.TableSync;

// Generic table-to-table sync engine over two MySQL connections.
// Given a query on the source connection, writes the result into the target table
// via staging + INSERT...ON DUPLICATE KEY UPDATE in a single atomic transaction.
//
// Caller contract:
//   - The query has aliases matching the target table column names.
//   - Every column to write must appear in the SELECT (with a placeholder value
//     if it will be overwritten by the onRow callback).
//   - The query MUST arrive fully parametrized: the engine does not bind anything.
//   - "The query is the truth": query NULLs are propagated as target NULLs.
//     To produce '' or other defaults, use COALESCE/IFNULL in the query itself.
//   - Transaction ownership: the engine NEVER opens nor commits transactions
//     on sourceConn. Reads on sourceConn participate in any pending transaction
//     of the caller. The atomic merge transaction on targetConn is fully
//     self-contained (START + COMMIT/ROLLBACK).
//
// Behavior:
//   - Target schema read via INFORMATION_SCHEMA (PK, types, generated columns).
//   - Auto-map query field -> target column by case-insensitive name equality.
//   - onRow runs BEFORE auto-map: paramByName* is text-replace; once a placeholder
//     is replaced it cannot be rebound. The callback claims fields it wants to
//     override; auto-map fills the remaining placeholders.
//   - Staging+merge atomic: INSERT...ON DUPLICATE KEY UPDATE (col=VALUES(col)
//     for all non-PK columns present in the query).
//   - When isDeleteOrphansEnabled=true: after merge, deletes from target the
//     records not present in the result via LEFT JOIN on PK. Sanity check:
//     rec_count must equal staging_count, otherwise an exception is raised.
//   - Errors -> exception with the target table name in the message.

interface

uses
  System.SysUtils, System.Classes,
  Data.DB,
  KLib.sqlstring, KLib.MySQL.Driver;

type
  // Called for every record: caller may override fields in row + use processed/total
  // for UI feedback. processed is the 1-based index of the current record being
  // processed; total is the source query record count (known after open).
  TTableSyncRowProc = reference to procedure(const query: TQuery; var row: sqlstring;
    processed: Integer; total: Integer);

function syncTable(
  const sourceConn: TConnection;
  const targetConn: TConnection;
  const targetTable: string;
  const sourceQuery: sqlstring;
  isDeleteOrphansEnabled: boolean;
  batchSize: Integer = 500;
  const onRow: TTableSyncRowProc = nil;
  const stagingTableName: string = ''
  ): Integer;

implementation

uses
  System.Variants,
  KLib.MySQL.Utils;

const
  DEFAULT_BATCH_SIZE = 500;
  STAGING_SUFFIX = '_staging';
  SCHEMA_QUERY_SQL =
    'SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA ' +
    'FROM INFORMATION_SCHEMA.COLUMNS ' +
    'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tbl ' +
    'ORDER BY ORDINAL_POSITION';
  COUNT_TABLE_SQL = 'SELECT COUNT(*) FROM ';

type
  TColumnInfo = record
    name: string;
    sqlType: string;
    isPk: boolean;
  end;

  TFieldDispatcher = record
    field: TField;
    column: TColumnInfo;
  end;

  TTableSyncRunner = class
  private
    sourceConnection: TConnection;
    targetConnection: TConnection;
    targetTableName: string;
    stagingName: string;
    sourceQueryText: sqlstring;
    effectiveBatchSize: Integer;
    isDeleteOrphansEnabled: boolean;
    onRowCallback: TTableSyncRowProc;

    schema: TArray<TColumnInfo>;
    dispatchers: TArray<TFieldDispatcher>;
    columns: TArray<string>;
    primaryKeys: TArray<string>;

    sourceCommand: TQuery;
    rowTemplate: string;
    stagingColumnsCsv: string;
    mergeSql: string;
    deleteOrphansSql: string;

    function readColumnInfo(const reader: TQuery): TColumnInfo;
    function joinCsv(const names: TArray<string>): string;
    function buildRowTemplate: string;
    function buildMergeSql: string;
    function buildDeleteOrphansSql: string;
    procedure bindRowParam(var row: sqlstring; const dispatcher: TFieldDispatcher);

    procedure loadSchema;
    procedure openSourceCommand;
    procedure buildDispatchers;
    procedure buildSqlFragments;
    procedure setupStagingTable;
    procedure flushBatch(var valuesSql: string; var batchCount: Integer);
    function processRecords: Integer;
    procedure mergeAtomic(processedCount: Integer);
    procedure dropStagingTableSilent;
  public
    constructor create(
      const aSourceConn: TConnection;
      const aTargetConn: TConnection;
      const aTargetTable: string;
      const aSourceQuery: sqlstring;
      aIsDeleteOrphansEnabled: boolean;
      aBatchSize: Integer;
      const aOnRow: TTableSyncRowProc;
      const aStagingTableName: string);
    destructor destroy; override;
    function execute: Integer;
  end;

constructor TTableSyncRunner.create(
  const aSourceConn: TConnection;
  const aTargetConn: TConnection;
  const aTargetTable: string;
  const aSourceQuery: sqlstring;
  aIsDeleteOrphansEnabled: boolean;
  aBatchSize: Integer;
  const aOnRow: TTableSyncRowProc;
  const aStagingTableName: string);
begin
  inherited create;
  sourceConnection := aSourceConn;
  targetConnection := aTargetConn;
  targetTableName := aTargetTable;
  sourceQueryText := aSourceQuery;
  isDeleteOrphansEnabled := aIsDeleteOrphansEnabled;
  onRowCallback := aOnRow;

  if aStagingTableName <> '' then
  begin
    stagingName := aStagingTableName;
  end
  else
  begin
    stagingName := aTargetTable + STAGING_SUFFIX;
  end;

  if aBatchSize > 0 then
  begin
    effectiveBatchSize := aBatchSize;
  end
  else
  begin
    effectiveBatchSize := DEFAULT_BATCH_SIZE;
  end;

  sourceCommand := nil;
end;

destructor TTableSyncRunner.destroy;
begin
  if Assigned(sourceCommand) then
  begin
    try
      sourceCommand.close;
    except
    end;
  end;
  FreeAndNil(sourceCommand);
  inherited;
end;

function TTableSyncRunner.readColumnInfo(const reader: TQuery): TColumnInfo;
var
  finalInfo: TColumnInfo;
begin
  finalInfo.name := reader.fieldbyname('COLUMN_NAME').asstring;
  finalInfo.sqlType := LowerCase(reader.fieldbyname('DATA_TYPE').asstring);
  finalInfo.isPk := SameText(reader.fieldbyname('COLUMN_KEY').asstring, 'PRI');

  Result := finalInfo;
end;

function TTableSyncRunner.joinCsv(const names: TArray<string>): string;
var
  _i: Integer;
  finalCsv: string;
begin
  finalCsv := '';
  for _i := 0 to High(names) do
  begin
    if _i > 0 then
    begin
      finalCsv := finalCsv + ',';
    end;
    finalCsv := finalCsv + names[_i];
  end;

  Result := finalCsv;
end;

function TTableSyncRunner.buildRowTemplate: string;
var
  _i: Integer;
  finalTemplate: string;
begin
  finalTemplate := '(';
  for _i := 0 to High(columns) do
  begin
    if _i > 0 then
    begin
      finalTemplate := finalTemplate + ',';
    end;
    finalTemplate := finalTemplate + ':' + columns[_i];
  end;
  finalTemplate := finalTemplate + ')';

  Result := finalTemplate;
end;

function TTableSyncRunner.buildMergeSql: string;
var
  _i: Integer;
  _j: Integer;
  _isPk: boolean;
  _setCsv: string;
  finalSql: string;
begin
  _setCsv := '';
  for _i := 0 to High(columns) do
  begin
    _isPk := false;
    for _j := 0 to High(primaryKeys) do
    begin
      if SameText(columns[_i], primaryKeys[_j]) then
      begin
        _isPk := true;
        Break;
      end;
    end;

    if not _isPk then
    begin
      if _setCsv <> '' then
      begin
        _setCsv := _setCsv + ',';
      end;
      _setCsv := _setCsv + columns[_i] + '=VALUES(' + columns[_i] + ')';
    end;
  end;

  finalSql := 'INSERT INTO ' + targetTableName + ' (' + joinCsv(columns) + ') ' +
    'SELECT ' + joinCsv(columns) + ' FROM ' + stagingName + ' ' +
    'ON DUPLICATE KEY UPDATE ' + _setCsv;

  Result := finalSql;
end;

function TTableSyncRunner.buildDeleteOrphansSql: string;
var
  _i: Integer;
  _onCsv: string;
  finalSql: string;
begin
  _onCsv := '';
  for _i := 0 to High(primaryKeys) do
  begin
    if _onCsv <> '' then
    begin
      _onCsv := _onCsv + ' AND ';
    end;
    _onCsv := _onCsv + 's.' + primaryKeys[_i] + '=a.' + primaryKeys[_i];
  end;

  finalSql := 'DELETE a FROM ' + targetTableName + ' a ' +
    'LEFT JOIN ' + stagingName + ' s ON ' + _onCsv + ' ' +
    'WHERE s.' + primaryKeys[0] + ' IS NULL';

  Result := finalSql;
end;

procedure TTableSyncRunner.bindRowParam(var row: sqlstring; const dispatcher: TFieldDispatcher);
var
  _sqlType: string;
  _columnName: string;
begin
  _columnName := dispatcher.column.name;

  if dispatcher.field.IsNull then
  begin
    row.paramByNameAsNull(_columnName);
    Exit;
  end;

  _sqlType := dispatcher.column.sqlType;
  if (_sqlType = 'varchar') or (_sqlType = 'char') or
    (_sqlType = 'text') or (_sqlType = 'tinytext') or (_sqlType = 'mediumtext') or
    (_sqlType = 'longtext') or (_sqlType = 'enum') or (_sqlType = 'set') then
  begin
    row.paramByNameAsString(_columnName, dispatcher.field.AsString);
  end
  else if (_sqlType = 'int') or (_sqlType = 'tinyint') or (_sqlType = 'smallint') or
    (_sqlType = 'mediumint') or (_sqlType = 'bigint') or (_sqlType = 'year') then
  begin
    row.paramByNameAsInteger(_columnName, dispatcher.field.AsInteger);
  end
  else if (_sqlType = 'decimal') or (_sqlType = 'numeric') or
    (_sqlType = 'float') or (_sqlType = 'double') then
  begin
    row.paramByNameAsFloat(_columnName, dispatcher.field.AsFloat);
  end
  else if (_sqlType = 'datetime') or (_sqlType = 'timestamp') then
  begin
    row.paramByNameAsDateTime(_columnName, dispatcher.field.AsDateTime);
  end
  else if _sqlType = 'date' then
  begin
    row.paramByNameAsDate(_columnName, dispatcher.field.AsDateTime);
  end
  else
  begin
    row.paramByNameAsString(_columnName, dispatcher.field.AsString);
  end;
end;

procedure TTableSyncRunner.loadSchema;
var
  _query: TQuery;
  _sql: sqlstring;
  _column: TColumnInfo;
  _extra: string;
  _count: Integer;
begin
  _sql := SCHEMA_QUERY_SQL;
  _sql.paramByNameAsString('tbl', targetTableName);

  SetLength(schema, 0);
  SetLength(primaryKeys, 0);
  _count := 0;

  _query := getTQuery(targetConnection, string(_sql));
  try
    _query.open;
    while not _query.eof do
    begin
      _extra := LowerCase(_query.fieldbyname('EXTRA').asstring);
      if Pos('generated', _extra) = 0 then
      begin
        _column := readColumnInfo(_query);
        SetLength(schema, _count + 1);
        schema[_count] := _column;
        Inc(_count);

        if _column.isPk then
        begin
          SetLength(primaryKeys, Length(primaryKeys) + 1);
          primaryKeys[High(primaryKeys)] := _column.name;
        end;
      end;
      _query.next;
    end;
  finally
    _query.close;
    FreeAndNil(_query);
  end;

  if Length(schema) = 0 then
  begin
    raise Exception.Create('target table "' + targetTableName +
      '" not found or has no insertable columns');
  end;
  if Length(primaryKeys) = 0 then
  begin
    raise Exception.Create('target table "' + targetTableName + '" has no primary key');
  end;
end;

procedure TTableSyncRunner.openSourceCommand;
begin
  sourceCommand := getTQuery(sourceConnection, string(sourceQueryText));
  sourceCommand.open;
end;

procedure TTableSyncRunner.buildDispatchers;
var
  _i: Integer;
  _j: Integer;
  _dispatcher: TFieldDispatcher;
  _field: TField;
  _isFound: boolean;
  _count: Integer;
begin
  SetLength(dispatchers, 0);
  SetLength(columns, 0);
  _count := 0;

  for _i := 0 to sourceCommand.FieldCount - 1 do
  begin
    _field := sourceCommand.Fields[_i];
    _isFound := false;

    for _j := 0 to High(schema) do
    begin
      if SameText(schema[_j].name, _field.FieldName) then
      begin
        _dispatcher.field := _field;
        _dispatcher.column := schema[_j];

        SetLength(dispatchers, _count + 1);
        dispatchers[_count] := _dispatcher;
        SetLength(columns, _count + 1);
        columns[_count] := schema[_j].name;
        Inc(_count);

        _isFound := true;
        Break;
      end;
    end;

    if not _isFound then
    begin
      raise Exception.Create('query field "' + _field.FieldName +
        '" does not match any column of "' + targetTableName + '"');
    end;
  end;

  if Length(columns) = 0 then
  begin
    raise Exception.Create('no query field of "' + targetTableName +
      '" matches a target column');
  end;
end;

procedure TTableSyncRunner.buildSqlFragments;
begin
  rowTemplate := buildRowTemplate;
  stagingColumnsCsv := joinCsv(columns);
  mergeSql := buildMergeSql;
  deleteOrphansSql := buildDeleteOrphansSql;
end;

procedure TTableSyncRunner.setupStagingTable;
begin
  targetConnection.executeQuery('DROP TABLE IF EXISTS ' + stagingName);
  targetConnection.executeQuery('CREATE TABLE ' + stagingName + ' LIKE ' + targetTableName);
  targetConnection.executeQuery('ALTER TABLE ' + stagingName + ' DISABLE KEYS');
end;

procedure TTableSyncRunner.flushBatch(var valuesSql: string; var batchCount: Integer);
begin
  if batchCount = 0 then
  begin
    Exit;
  end;

  targetConnection.executeQuery('INSERT INTO ' + stagingName + ' (' +
    stagingColumnsCsv + ') VALUES ' + valuesSql);
  valuesSql := '';
  batchCount := 0;
end;

function TTableSyncRunner.processRecords: Integer;
var
  _row: sqlstring;
  _valuesSql: string;
  _batchCount: Integer;
  _processed: Integer;
  _total: Integer;
  _i: Integer;
begin
  _valuesSql := '';
  _batchCount := 0;
  _processed := 0;
  _total := sourceCommand.RecordCount;

  while not sourceCommand.eof do
  begin
    Inc(_processed);

    _row := rowTemplate;
    if Assigned(onRowCallback) then
    begin
      onRowCallback(sourceCommand, _row, _processed, _total);
    end;
    for _i := 0 to High(dispatchers) do
    begin
      bindRowParam(_row, dispatchers[_i]);
    end;

    if _batchCount > 0 then
    begin
      _valuesSql := _valuesSql + ',' + string(_row);
    end
    else
    begin
      _valuesSql := string(_row);
    end;
    Inc(_batchCount);

    if _batchCount >= effectiveBatchSize then
    begin
      flushBatch(_valuesSql, _batchCount);
    end;

    sourceCommand.next;
  end;

  flushBatch(_valuesSql, _batchCount);

  targetConnection.executeQuery('ALTER TABLE ' + stagingName + ' ENABLE KEYS');

  Result := _processed;
end;

procedure TTableSyncRunner.mergeAtomic(processedCount: Integer);
var
  _stagingRecordCount: Integer;
begin
  targetConnection.executeQuery('START TRANSACTION');
  try
    targetConnection.executeQuery(mergeSql);

    if isDeleteOrphansEnabled then
    begin
      _stagingRecordCount := StrToIntDef(VarToStr(
        targetConnection.getFirstFieldFromSQLStatement(COUNT_TABLE_SQL + stagingName)), 0);

      if (processedCount > 0) and (_stagingRecordCount = processedCount) then
      begin
        targetConnection.executeQuery(deleteOrphansSql);
      end
      else if processedCount > 0 then
      begin
        raise Exception.Create('DELETE orphans: anomaly read=' +
          IntToStr(processedCount) + ' staging=' + IntToStr(_stagingRecordCount) +
          ' on "' + targetTableName + '"');
      end;
    end;

    targetConnection.executeQuery('COMMIT');
  except
    on e: Exception do
    begin
      try
        targetConnection.executeQuery('ROLLBACK');
      except
      end;
      raise;
    end;
  end;
end;

procedure TTableSyncRunner.dropStagingTableSilent;
begin
  try
    targetConnection.executeQuery('DROP TABLE IF EXISTS ' + stagingName);
  except
  end;
end;

function TTableSyncRunner.execute: Integer;
var
  _processedCount: Integer;
begin
  _processedCount := 0;
  try
    loadSchema;
    openSourceCommand;
    buildDispatchers;
    buildSqlFragments;
    setupStagingTable;
    _processedCount := processRecords;
    mergeAtomic(_processedCount);
  finally
    dropStagingTableSilent;
  end;

  Result := _processedCount;
end;

function syncTable(
  const sourceConn: TConnection;
  const targetConn: TConnection;
  const targetTable: string;
  const sourceQuery: sqlstring;
  isDeleteOrphansEnabled: boolean;
  batchSize: Integer = 500;
  const onRow: TTableSyncRowProc = nil;
  const stagingTableName: string = ''
  ): Integer;
var
  _runner: TTableSyncRunner;
begin
  _runner := TTableSyncRunner.create(sourceConn, targetConn, targetTable,
    sourceQuery, isDeleteOrphansEnabled, batchSize, onRow, stagingTableName);
  try
    Result := _runner.execute;
  finally
    FreeAndNil(_runner);
  end;
end;

end.
