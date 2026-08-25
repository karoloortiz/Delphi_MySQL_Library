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

unit KLib.MySQL.DynamicTable;

interface

uses
  System.TypInfo, System.Generics.Collections,
  KLib.Constants,
  KLib.MySQL.Driver;

type
{$scopedenums ON}
  TTableType = (temporary, permanent);
{$scopedenums OFF}

  TDynamicTable = class
  private
    _connection: TConnection;
    _tableName: string;
    _tableType: TTableType;
    _isCreated: boolean;

    procedure setOrGenerateTableName(tableName: string);
    function buildCreateTableSQL(selectQuery: string): string; overload;
    function buildCreateTableSQL(dataTypeInfo: PTypeInfo): string; overload;
    function buildDropTableSQL: string;
    procedure executeCreateTable(createSQL: string);

  public
    isKeepEnabled: boolean;

    property tableName: string read _tableName;
    property tableType: TTableType read _tableType;
    property isCreated: boolean read _isCreated;

    constructor create(connection: TConnection; tableType: TTableType = TTableType.temporary);
    procedure execute(selectQuery: string; tableName: string = EMPTY_STRING); overload;
    procedure execute<T>(data: TArray<T>; tableName: string = EMPTY_STRING); overload;
    procedure execute<T>(data: TList<T>; tableName: string = EMPTY_STRING); overload;
    procedure drop;
    destructor Destroy; override;
  end;

implementation

uses
  System.SysUtils, System.Rtti,
  KLib.Validate, KLib.Utils, KLib.StringUtils,
  KLib.MySQL.Utils, KLib.MySQL.BatchInsert;

constructor TDynamicTable.create(connection: TConnection; tableType: TTableType = TTableType.temporary);
begin
  _connection := connection;
  _tableType := tableType;
  _isCreated := false;
  _tableName := EMPTY_STRING;
  isKeepEnabled := false;
end;

procedure TDynamicTable.setOrGenerateTableName(tableName: string);
begin
  if tableName = EMPTY_STRING then
  begin
    _tableName := getRandString();
  end
  else
  begin
    _tableName := tableName;
  end;
end;

function TDynamicTable.buildCreateTableSQL(selectQuery: string): string;
const
  CREATE_TEMPORARY_TABLE_TEMPLATE = 'CREATE TEMPORARY TABLE %s %s';
  CREATE_TABLE_TEMPLATE = 'CREATE TABLE %s %s';
var
  _template: string;
begin
  validateThatStringIsNotEmpty(_tableName, 'Table name cannot be empty');
  validateThatStringIsNotEmpty(selectQuery, 'Query statement cannot be empty');

  if _tableType = TTableType.temporary then
  begin
    _template := CREATE_TEMPORARY_TABLE_TEMPLATE;
  end;
  if _tableType = TTableType.permanent then
  begin
    _template := CREATE_TABLE_TEMPLATE;
  end;

  Result := Format(_template, [getQuotedTableName(_tableName), selectQuery]);
end;

function TDynamicTable.buildCreateTableSQL(dataTypeInfo: PTypeInfo): string;
const
  CREATE_TEMPORARY_TABLE_TEMPLATE = 'CREATE TEMPORARY TABLE %s (%s)';
  CREATE_TABLE_TEMPLATE = 'CREATE TABLE %s (%s)';
var
  _ctx: TRttiContext;
  _columnDefinitions: string;
  _template: string;
begin
  validateThatStringIsNotEmpty(_tableName, 'Table name cannot be empty');
  _columnDefinitions := getColumnDefinitions(_ctx.GetType(dataTypeInfo));

  if _tableType = TTableType.temporary then
  begin
    _template := CREATE_TEMPORARY_TABLE_TEMPLATE;
  end;
  if _tableType = TTableType.permanent then
  begin
    _template := CREATE_TABLE_TEMPLATE;
  end;

  Result := Format(_template, [getQuotedTableName(_tableName), _columnDefinitions]);
end;

function TDynamicTable.buildDropTableSQL: string;
const
  DROP_TEMPORARY_TABLE_TEMPLATE = 'DROP TEMPORARY TABLE %s';
  DROP_TABLE_TEMPLATE = 'DROP TABLE %s';
var
  _template: string;
begin
  validateThatStringIsNotEmpty(_tableName, 'Table name cannot be empty');

  if _tableType = TTableType.temporary then
  begin
    _template := DROP_TEMPORARY_TABLE_TEMPLATE;
  end;
  if _tableType = TTableType.permanent then
  begin
    _template := DROP_TABLE_TEMPLATE;
  end;

  Result := Format(_template, [getQuotedTableName(_tableName)]);
end;

procedure TDynamicTable.executeCreateTable(createSQL: string);
begin
  KLib.MySQL.Utils.executeQuery(createSQL, _connection);
  _isCreated := true;
end;

procedure TDynamicTable.execute(selectQuery: string; tableName: string = EMPTY_STRING);
var
  _createSQL: string;
begin
  drop;
  setOrGenerateTableName(tableName);
  _createSQL := buildCreateTableSQL(selectQuery);
  executeCreateTable(_createSQL);
end;

procedure TDynamicTable.execute<T>(data: TArray<T>; tableName: string = EMPTY_STRING);
var
  _createSQL: string;
begin
  drop;
  setOrGenerateTableName(tableName);
  _createSQL := buildCreateTableSQL(TypeInfo(T));
  executeCreateTable(_createSQL);
  TBatchInsert.into<T>(_connection, _tableName, data);
end;

procedure TDynamicTable.execute<T>(data: TList<T>; tableName: string = EMPTY_STRING);
var
  _createSQL: string;
begin
  drop;
  setOrGenerateTableName(tableName);
  _createSQL := buildCreateTableSQL(TypeInfo(T));
  executeCreateTable(_createSQL);
  if data <> nil then
  begin
    TBatchInsert.into<T>(_connection, _tableName, data.ToArray);
  end;
end;

procedure TDynamicTable.drop;
var
  _dropSQL: string;
begin
  if isCreated then
  begin
    _dropSQL := buildDropTableSQL;
    KLib.MySQL.Utils.executeQuery(_dropSQL, _connection);
    _isCreated := false;
  end;
end;

destructor TDynamicTable.destroy;
begin
  if (_tableType = TTableType.temporary) or (not isKeepEnabled) then
  begin
    drop;
  end;
  inherited;
end;

end.
