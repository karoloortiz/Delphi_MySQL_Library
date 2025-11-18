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
  System.Rtti, System.TypInfo, System.Generics.Collections,
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
    procedure insertDataIntoTable(dataTypeInfo: PTypeInfo; dataValues: TArray<TValue>);
    function mapToMySQLType(rttiType: TRttiType): string;
    function formatValueForSQL(value: TValue; fieldType: TRttiType): string;
    procedure executeBatchInsert(data: TArray<TValue>; dataTypeInfo: PTypeInfo);
    function buildFieldDefinitionsFromType(rttiType: TRttiType): string;
    function buildFieldDefinitionsFromRecord(rttiType: TRttiType): string;
    function buildFieldDefinitionsFromClass(rttiType: TRttiType): string;
    class function convertToTValueArray<T>(data: TArray<T>): TArray<TValue>; overload; static;
    class function convertToTValueArray<T>(data: TList<T>): TArray<TValue>; overload; static;

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
  System.SysUtils, System.StrUtils, System.Variants, System.Classes,
  KLib.sqlstring, KLib.Validate, KLib.Utils, KLib.Generics.Attributes,
  KLib.StringUtils, KLib.DateTimeUtils,
  KLib.MySQL.Utils;

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
  CREATE_TEMPORARY_TABLE_TEMPLATE = 'CREATE TEMPORARY TABLE `%s` %s';
  CREATE_TABLE_TEMPLATE = 'CREATE TABLE `%s` %s';
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

  Result := Format(_template, [_tableName, selectQuery]);
end;

function TDynamicTable.buildCreateTableSQL(dataTypeInfo: PTypeInfo): string;
const
  CREATE_TEMPORARY_TABLE_TEMPLATE = 'CREATE TEMPORARY TABLE `%s` (%s)';
  CREATE_TABLE_TEMPLATE = 'CREATE TABLE `%s` (%s)';
var
  _ctx: TRttiContext;
  _rttiType: TRttiType;
  _fieldsStr: string;
  _template: string;
begin
  validateThatStringIsNotEmpty(_tableName, 'Table name cannot be empty');
  _rttiType := _ctx.GetType(dataTypeInfo);
  _fieldsStr := buildFieldDefinitionsFromType(_rttiType);

  if _tableType = TTableType.temporary then
  begin
    _template := CREATE_TEMPORARY_TABLE_TEMPLATE;
  end;
  if _tableType = TTableType.permanent then
  begin
    _template := CREATE_TABLE_TEMPLATE;
  end;

  Result := Format(_template, [_tableName, _fieldsStr]);
end;

function TDynamicTable.buildFieldDefinitionsFromType(rttiType: TRttiType): string;
var
  _result: string;
begin
  if rttiType.TypeKind = tkRecord then
  begin
    _result := buildFieldDefinitionsFromRecord(rttiType);
  end
  else if rttiType.TypeKind = tkClass then
  begin
    _result := buildFieldDefinitionsFromClass(rttiType);
  end
  else
  begin
    raise Exception.Create('Type must be a record or class');
  end;

  if _result = EMPTY_STRING then
  begin
    raise Exception.Create('No fields found in type');
  end;

  Result := _result;
end;

function TDynamicTable.buildFieldDefinitionsFromRecord(rttiType: TRttiType): string;
var
  _field: TRttiField;
  _fields: TStringList;
  _fieldName: string;
  _fieldType: string;
  _customName: string;
  i: integer;
begin
  _fields := TStringList.Create;
  try
    for _field in rttiType.GetFields do
    begin
      if _field.GetAttribute<IgnoreAttribute> <> nil then
      begin
        Continue;
      end;

      _fieldName := _field.Name;
      if _field.GetAttribute<CustomNameAttribute> <> nil then
      begin
        _customName := _field.GetAttribute<CustomNameAttribute>.Value;
        _fieldName := _customName;
      end;

      _fieldType := mapToMySQLType(_field.FieldType);
      _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
    end;

    Result := EMPTY_STRING;
    for i := 0 to _fields.Count - 1 do
    begin
      if i > 0 then
      begin
        Result := Result + ', ';
      end;
      Result := Result + _fields[i];
    end;
  finally
    FreeAndNil(_fields);
  end;
end;

function TDynamicTable.buildFieldDefinitionsFromClass(rttiType: TRttiType): string;
var
  _field: TRttiField;
  _prop: TRttiProperty;
  _fields: TStringList;
  _fieldName: string;
  _fieldType: string;
  _customName: string;
  i: integer;
begin
  _fields := TStringList.Create;
  try
    for _field in (rttiType as TRttiInstanceType).GetFields do
    begin
      if not(_field.Visibility in [mvPublic, mvPublished]) then
      begin
        Continue;
      end;

      if _field.GetAttribute<IgnoreAttribute> <> nil then
      begin
        Continue;
      end;

      _fieldName := _field.Name;
      if _field.GetAttribute<CustomNameAttribute> <> nil then
      begin
        _customName := _field.GetAttribute<CustomNameAttribute>.Value;
        _fieldName := _customName;
      end;

      _fieldType := mapToMySQLType(_field.FieldType);
      _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
    end;

    for _prop in (rttiType as TRttiInstanceType).GetProperties do
    begin
      if not(_prop.Visibility in [mvPublic, mvPublished]) then
      begin
        Continue;
      end;

      if not _prop.IsReadable then
      begin
        Continue;
      end;

      if _prop.GetAttribute<IgnoreAttribute> <> nil then
      begin
        Continue;
      end;

      _fieldName := _prop.Name;
      if _prop.GetAttribute<CustomNameAttribute> <> nil then
      begin
        _customName := _prop.GetAttribute<CustomNameAttribute>.Value;
        _fieldName := _customName;
      end;

      _fieldType := mapToMySQLType(_prop.PropertyType);
      _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
    end;

    Result := EMPTY_STRING;
    for i := 0 to _fields.Count - 1 do
    begin
      if i > 0 then
      begin
        Result := Result + ', ';
      end;
      Result := Result + _fields[i];
    end;
  finally
    FreeAndNil(_fields);
  end;
end;

function TDynamicTable.buildDropTableSQL: string;
const
  DROP_TEMPORARY_TABLE_TEMPLATE = 'DROP TEMPORARY TABLE `%s`';
  DROP_TABLE_TEMPLATE = 'DROP TABLE `%s`';
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

  Result := Format(_template, [_tableName]);
end;

procedure TDynamicTable.executeCreateTable(createSQL: string);
begin
  KLib.MySQL.Utils.executeQuery(createSQL, _connection);
  _isCreated := true;
end;

procedure TDynamicTable.insertDataIntoTable(dataTypeInfo: PTypeInfo; dataValues: TArray<TValue>);
begin
  if Length(dataValues) = 0 then
  begin
    raise Exception.Create('Data array cannot be empty');
  end;
  executeBatchInsert(dataValues, dataTypeInfo);
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
  _dataAsValues: TArray<TValue>;
  _createSQL: string;
begin
  drop;
  setOrGenerateTableName(tableName);
  _dataAsValues := convertToTValueArray<T>(data);
  _createSQL := buildCreateTableSQL(TypeInfo(T));
  executeCreateTable(_createSQL);
  insertDataIntoTable(TypeInfo(T), _dataAsValues);
end;

procedure TDynamicTable.execute<T>(data: TList<T>; tableName: string = EMPTY_STRING);
var
  _dataAsValues: TArray<TValue>;
  _createSQL: string;
begin
  drop;
  setOrGenerateTableName(tableName);
  _dataAsValues := convertToTValueArray<T>(data);
  _createSQL := buildCreateTableSQL(TypeInfo(T));
  executeCreateTable(_createSQL);
  insertDataIntoTable(TypeInfo(T), _dataAsValues);
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

class function TDynamicTable.convertToTValueArray<T>(data: TArray<T>): TArray<TValue>;
var
  _result: TArray<TValue>;
  _value: TValue;
  i: integer;
begin
  SetLength(_result, Length(data));
  for i := 0 to High(data) do
  begin
    TValue.Make(@data[i], TypeInfo(T), _value);
    _result[i] := _value;
  end;
  Result := _result;
end;

class function TDynamicTable.convertToTValueArray<T>(data: TList<T>): TArray<TValue>;
var
  _result: TArray<TValue>;
  _value: TValue;
  _item: T;
  i: integer;
begin
  SetLength(_result, data.Count);
  for i := 0 to data.Count - 1 do
  begin
    _item := data[i];
    TValue.Make(@_item, TypeInfo(T), _value);
    _result[i] := _value;
  end;
  Result := _result;
end;

function TDynamicTable.mapToMySQLType(rttiType: TRttiType): string;
var
  _result: string;
begin
  _result := 'TEXT';

  case rttiType.TypeKind of
    tkInteger, tkInt64:
      _result := 'BIGINT';

    tkFloat:
      begin
        if rttiType.Handle = TypeInfo(TDateTime) then
        begin
          _result := 'DATETIME';
        end
        else if rttiType.Handle = TypeInfo(TDate) then
        begin
          _result := 'DATE';
        end
        else
        begin
          _result := 'DOUBLE';
        end;
      end;

    tkString, tkLString, tkWString, tkUString:
      _result := 'TEXT';

    tkEnumeration:
      begin
        if rttiType.Handle = TypeInfo(Boolean) then
        begin
          _result := 'TINYINT(1)';
        end
        else
        begin
          _result := 'VARCHAR(50)';
        end;
      end;

    tkChar, tkWChar:
      _result := 'CHAR(1)';

    tkVariant:
      _result := 'TEXT';

    tkRecord, tkClass:
      _result := 'TEXT';

  else
    _result := 'TEXT';
  end;

  Result := _result;
end;

function TDynamicTable.formatValueForSQL(value: TValue; fieldType: TRttiType): string;
var
  _result: string;
  _dateTime: TDateTime;
  _isNull: boolean;
begin
  _isNull := value.IsEmpty;

  if _isNull then
  begin
    _result := 'NULL';
  end
  else
  begin
    case fieldType.TypeKind of
      tkInteger, tkInt64:
        _result := value.AsInteger.ToString;

      tkFloat:
        begin
          if fieldType.Handle = TypeInfo(TDateTime) then
          begin
            _dateTime := value.AsType<TDateTime>;
            _result := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', _dateTime));
          end
          else if fieldType.Handle = TypeInfo(TDate) then
          begin
            _dateTime := value.AsType<TDate>;
            _result := QuotedStr(FormatDateTime('yyyy-mm-dd', _dateTime));
          end
          else
          begin
            _result := StringReplace(value.AsExtended.ToString, ',', '.', [rfReplaceAll]);
          end;
        end;

      tkString, tkLString, tkWString, tkUString:
        _result := QuotedStr(value.AsString);

      tkEnumeration:
        begin
          if fieldType.Handle = TypeInfo(Boolean) then
          begin
            if value.AsBoolean then
            begin
              _result := '1';
            end
            else
            begin
              _result := '0';
            end;
          end
          else
          begin
            _result := QuotedStr(value.ToString);
          end;
        end;

      tkChar, tkWChar:
        _result := QuotedStr(value.ToString);

      tkVariant:
        _result := QuotedStr(VarToStr(value.AsVariant));

    else
      _result := QuotedStr(value.ToString);
    end;
  end;

  Result := _result;
end;

procedure TDynamicTable.executeBatchInsert(data: TArray<TValue>; dataTypeInfo: PTypeInfo);
const
  INSERT_TEMPLATE = 'INSERT INTO `%s` VALUES %s';
  BATCH_SIZE = 1000;
var
  _ctx: TRttiContext;
  _rttiType: TRttiType;
  _field: TRttiField;
  _prop: TRttiProperty;
  _batchCount: integer;
  _valuesList: TStringList;
  _insertStatement: string;
  _rowValues: TStringList;
  _fieldValue: TValue;
  _fieldValueStr: string;
  _dataItem: TValue;
  _recordPtr: Pointer;
  _classInstance: TObject;
  i: integer;
  j: integer;
begin
  if Length(data) = 0 then
  begin
    Exit;
  end;

  _valuesList := TStringList.Create;
  _rowValues := TStringList.Create;
  try
    _batchCount := 0;
    _rttiType := _ctx.GetType(dataTypeInfo);

    for i := 0 to High(data) do
    begin
      _dataItem := data[i];
      _rowValues.Clear;

      if _rttiType.TypeKind = tkRecord then
      begin
        _recordPtr := _dataItem.GetReferenceToRawData;

        for _field in _rttiType.GetFields do
        begin
          if _field.GetAttribute<IgnoreAttribute> <> nil then
          begin
            Continue;
          end;

          _fieldValue := _field.GetValue(_recordPtr);
          _fieldValueStr := formatValueForSQL(_fieldValue, _field.FieldType);
          _rowValues.Add(_fieldValueStr);
        end;
      end
      else if _rttiType.TypeKind = tkClass then
      begin
        _classInstance := _dataItem.AsObject;

        if _classInstance = nil then
        begin
          Continue;
        end;

        for _field in (_rttiType as TRttiInstanceType).GetFields do
        begin
          if not(_field.Visibility in [mvPublic, mvPublished]) then
          begin
            Continue;
          end;

          if _field.GetAttribute<IgnoreAttribute> <> nil then
          begin
            Continue;
          end;

          try
            _fieldValue := _field.GetValue(_classInstance);
            _fieldValueStr := formatValueForSQL(_fieldValue, _field.FieldType);
            _rowValues.Add(_fieldValueStr);
          except
            _rowValues.Add('NULL');
          end;
        end;

        for _prop in (_rttiType as TRttiInstanceType).GetProperties do
        begin
          if not(_prop.Visibility in [mvPublic, mvPublished]) then
          begin
            Continue;
          end;

          if not _prop.IsReadable then
          begin
            Continue;
          end;

          if _prop.GetAttribute<IgnoreAttribute> <> nil then
          begin
            Continue;
          end;

          try
            _fieldValue := _prop.GetValue(_classInstance);
            _fieldValueStr := formatValueForSQL(_fieldValue, _prop.PropertyType);
            _rowValues.Add(_fieldValueStr);
          except
            _rowValues.Add('NULL');
          end;
        end;
      end;

      if _rowValues.Count > 0 then
      begin
        _fieldValueStr := '';
        for j := 0 to _rowValues.Count - 1 do
        begin
          if j > 0 then
          begin
            _fieldValueStr := _fieldValueStr + ', ';
          end;
          _fieldValueStr := _fieldValueStr + _rowValues[j];
        end;
        _valuesList.Add('(' + _fieldValueStr + ')');
        _batchCount := _batchCount + 1;
      end;

      if (_batchCount >= BATCH_SIZE) or (i = High(data)) then
      begin
        if _valuesList.Count > 0 then
        begin
          _fieldValueStr := '';
          for j := 0 to _valuesList.Count - 1 do
          begin
            if j > 0 then
            begin
              _fieldValueStr := _fieldValueStr + ', ';
            end;
            _fieldValueStr := _fieldValueStr + _valuesList[j];
          end;
          _insertStatement := Format(INSERT_TEMPLATE, [_tableName, _fieldValueStr]);
          KLib.MySQL.Utils.executeQuery(_insertStatement, _connection);
          _valuesList.Clear;
          _batchCount := 0;
        end;
      end;
    end;
  finally
    FreeAndNil(_valuesList);
    FreeAndNil(_rowValues);
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
