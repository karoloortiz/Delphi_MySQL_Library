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

unit KLib.MySQL.TemporaryTable;

interface

uses
  System.Rtti, System.TypInfo,
  KLib.Constants,
  KLib.MySQL.Driver;

type
  TTemporaryTable = class
  private
    _connection: TConnection;
    _selectQueryStmt: string;
    _isCreated: boolean;

    procedure initialize(tableName: string; connection: TConnection = nil; selectQueryStmt: string = EMPTY_STRING);
    function getMySQLTypeFromRttiType(rttiType: TRttiType): string;
    function getValueAsString(value: TValue; fieldType: TRttiType): string;
    function getCreateTableFromTypeInfo_SQLStmt(typeInfo: PTypeInfo): string;
    procedure insertDataBatch(data: TArray<TValue>; batchSize: integer);

  public
    tableName: String;
    property isCreated: boolean read _isCreated;

    constructor create(connection: TConnection; tableName: string = EMPTY_STRING; selectQueryStmt: string = EMPTY_STRING);
    procedure recreate(selectQueryStmt: string; tableName: string = EMPTY_STRING; connection: TConnection = nil); overload;
    procedure recreate<T>(data: TArray<T>; tableName: string = EMPTY_STRING; connection: TConnection = nil); overload;
    procedure execute;
    procedure drop;
    destructor Destroy; override;
  end;

function getCreateTemporaryTableFromQuery_SQLStmt(const tableName: string; const queryStmt: string): string;
function getDropTemporaryTable_SQLStmt(const tableName: string): string;

implementation

uses
  System.SysUtils, System.StrUtils, System.Variants, System.Classes,
  KLib.sqlstring, KLib.Validate, KLib.Utils, KLib.Generics.Attributes,
  KLib.StringUtils, KLib.DateTimeUtils,
  KLib.MySQL.Utils;

function getCreateTemporaryTableFromQuery_SQLStmt(const tableName: string; const queryStmt: string): string;
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
  validateThatStringIsNotEmpty(tableName, 'Table name cannot be empty');
  validateThatStringIsNotEmpty(queryStmt, 'Query statement cannot be empty');
  _queryStmt := CREATE_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME_PARAM_QUERYSTMT;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);
  _queryStmt.setParamAsString(PARAM_QUERYSTMT, queryStmt);

  Result := _queryStmt;
end;

function getDropTemporaryTable_SQLStmt(const tableName: string): string;
const
  PARAM_TABLENAME = ':TABLENAME';
  DROP_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME =
    'DROP TEMPORARY TABLE' + sLineBreak +
    PARAM_TABLENAME;
var
  _queryStmt: sqlstring;
begin
  validateThatStringIsNotEmpty(tableName, 'Table name cannot be empty');
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
  if (Self.tableName = EMPTY_STRING) then
  begin
    Self.tableName := getRandString();
  end;
  Self._selectQueryStmt := selectQueryStmt;
  if (connection <> nil) then
  begin
    Self._connection := connection;
  end;
  _isCreated := false;
end;

procedure TTemporaryTable.execute;
const
  ERR_MSG = 'Temporary table already created.';
var
  _queryStmt: string;
begin
  if isCreated then
  begin
    raise Exception.Create(ERR_MSG);
  end;
  _queryStmt := getCreateTemporaryTableFromQuery_SQLStmt(tableName, _selectQueryStmt);
  executeQuery(_queryStmt, _connection);

  _isCreated := true;
end;

procedure TTemporaryTable.drop;
var
  _queryStmt: string;
begin
  if isCreated then
  begin
    _queryStmt := getDropTemporaryTable_SQLStmt(tableName);
    executeQuery(_queryStmt, _connection);
    _isCreated := false;
  end;
end;

function TTemporaryTable.getMySQLTypeFromRttiType(rttiType: TRttiType): string;
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

function TTemporaryTable.getValueAsString(value: TValue; fieldType: TRttiType): string;
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

function TTemporaryTable.getCreateTableFromTypeInfo_SQLStmt(typeInfo: PTypeInfo): string;
const
  CREATE_TABLE_TEMPLATE = 'CREATE TEMPORARY TABLE `%s` (%s)';
var
  _result: string;
  _ctx: TRttiContext;
  _rttiType: TRttiType;
  _field: TRttiField;
  _prop: TRttiProperty;
  _fields: TStringList;
  _fieldName: string;
  _fieldType: string;
  _customName: string;
  _fieldsStr: string;
  i: integer;
begin
  _fields := TStringList.Create;
  try
    _rttiType := _ctx.GetType(typeInfo);

    if _rttiType.TypeKind = tkRecord then
    begin
      for _field in _rttiType.GetFields do
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

        _fieldType := getMySQLTypeFromRttiType(_field.FieldType);
        _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
      end;
    end
    else if _rttiType.TypeKind = tkClass then
    begin
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

        _fieldName := _field.Name;
        if _field.GetAttribute<CustomNameAttribute> <> nil then
        begin
          _customName := _field.GetAttribute<CustomNameAttribute>.Value;
          _fieldName := _customName;
        end;

        _fieldType := getMySQLTypeFromRttiType(_field.FieldType);
        _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
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

        _fieldName := _prop.Name;
        if _prop.GetAttribute<CustomNameAttribute> <> nil then
        begin
          _customName := _prop.GetAttribute<CustomNameAttribute>.Value;
          _fieldName := _customName;
        end;

        _fieldType := getMySQLTypeFromRttiType(_prop.PropertyType);
        _fields.Add(Format('`%s` %s', [_fieldName, _fieldType]));
      end;
    end
    else
    begin
      raise Exception.Create('Type must be a record or class');
    end;

    if _fields.Count = 0 then
    begin
      raise Exception.Create('No fields found in type');
    end;

    _fieldsStr := '';
    for i := 0 to _fields.Count - 1 do
    begin
      if i > 0 then
      begin
        _fieldsStr := _fieldsStr + ', ';
      end;
      _fieldsStr := _fieldsStr + _fields[i];
    end;

    _result := Format(CREATE_TABLE_TEMPLATE, [tableName, _fieldsStr]);
  finally
    FreeAndNil(_fields);
  end;

  Result := _result;
end;

procedure TTemporaryTable.insertDataBatch(data: TArray<TValue>; batchSize: integer);
const
  INSERT_TEMPLATE = 'INSERT INTO `%s` VALUES %s';
  MAX_BATCH_SIZE = 1000;
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
  _actualBatchSize: integer;
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

  _actualBatchSize := batchSize;
  if _actualBatchSize <= 0 then
  begin
    _actualBatchSize := MAX_BATCH_SIZE;
  end;

  _valuesList := TStringList.Create;
  _rowValues := TStringList.Create;
  try
    _batchCount := 0;
    _rttiType := _ctx.GetType(data[0].TypeInfo);

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
          _fieldValueStr := getValueAsString(_fieldValue, _field.FieldType);
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
            _fieldValueStr := getValueAsString(_fieldValue, _field.FieldType);
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
            _fieldValueStr := getValueAsString(_fieldValue, _prop.PropertyType);
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

      if (_batchCount >= _actualBatchSize) or (i = High(data)) then
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
          _insertStatement := Format(INSERT_TEMPLATE, [tableName, _fieldValueStr]);
          executeQuery(_insertStatement, _connection);
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

procedure TTemporaryTable.recreate<T>(data: TArray<T>; tableName: string = EMPTY_STRING; connection: TConnection = nil);
var
  _tableName: string;
  _createTableStmt: string;
  _dataAsValues: TArray<TValue>;
  _value: TValue;
  i: integer;
begin
  Self.drop;

  _tableName := tableName;
  if _tableName = EMPTY_STRING then
  begin
    _tableName := Self.tableName;
  end;

  if connection <> nil then
  begin
    Self._connection := connection;
  end;

  Self.tableName := _tableName;
  if Self.tableName = EMPTY_STRING then
  begin
    Self.tableName := getRandString();
  end;

  if Length(data) = 0 then
  begin
    raise Exception.Create('Data array cannot be empty');
  end;

  _createTableStmt := getCreateTableFromTypeInfo_SQLStmt(TypeInfo(T));
  executeQuery(_createTableStmt, _connection);
  _isCreated := true;

  SetLength(_dataAsValues, Length(data));
  for i := 0 to High(data) do
  begin
    TValue.Make(@data[i], TypeInfo(T), _value);
    _dataAsValues[i] := _value;
  end;

  insertDataBatch(_dataAsValues, 1000);
end;

destructor TTemporaryTable.destroy;
begin
  drop;
  inherited;
end;

end.
