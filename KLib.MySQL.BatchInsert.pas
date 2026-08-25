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

unit KLib.MySQL.BatchInsert;

interface

uses
  System.Rtti, System.TypInfo, System.Generics.Collections,
  KLib.MySQL.Driver;

const
  DEFAULT_BATCH_SIZE = 1000;
  MAX_BATCH_LENGTH_IN_CHARS = 1024 * 1024;

type
  TMemberInfo = record
    columnName: string;
    memberType: TRttiType;
    field: TRttiField; //one of field/prop is set
    prop: TRttiProperty;
  end;

  //generic holder (Delphi has no free generic routines): TBatchInsert.into<T>(...)
  TBatchInsert = record
    class procedure into<T>(connection: TConnection; tableName: string;
      data: TArray<T>; batchSize: integer = DEFAULT_BATCH_SIZE); overload; static;
    class procedure into<T>(connection: TConnection; tableName: string;
      data: TList<T>; batchSize: integer = DEFAULT_BATCH_SIZE); overload; static;
  end;

//core: insert an array of already-boxed records into an existing table
procedure batchInsertValues(connection: TConnection; tableName: string;
  data: TArray<TValue>; typeInfo: PTypeInfo; batchSize: integer = DEFAULT_BATCH_SIZE);

//shared type -> columns mapping, so CREATE TABLE and INSERT never diverge
function getInsertableMembers(rttiType: TRttiType): TArray<TMemberInfo>;
function getColumnDefinitions(rttiType: TRttiType): string;
function getColumnsCsv(members: TArray<TMemberInfo>): string;
function getQuotedTableName(tableName: string): string;
function getQuotedIdentifier(identifier: string): string;
function mapToMySQLType(rttiType: TRttiType): string;
function formatValueForSQL(value: TValue; fieldType: TRttiType): string;

implementation

uses
  System.SysUtils, System.Variants,
  KLib.Constants, KLib.Generics.Attributes, KLib.StringUtils, KLib.Validate,
  KLib.MySQL.Utils;

{ TBatchInsert }

class procedure TBatchInsert.into<T>(connection: TConnection; tableName: string;
  data: TArray<T>; batchSize: integer = DEFAULT_BATCH_SIZE);
var
  _values: TArray<TValue>;
  _value: TValue;
  i: integer;
begin
  SetLength(_values, Length(data));
  for i := 0 to High(data) do
  begin
    TValue.Make(@data[i], TypeInfo(T), _value);
    _values[i] := _value;
  end;

  batchInsertValues(connection, tableName, _values, TypeInfo(T), batchSize);
end;

class procedure TBatchInsert.into<T>(connection: TConnection; tableName: string;
  data: TList<T>; batchSize: integer = DEFAULT_BATCH_SIZE);
begin
  if data <> nil then
  begin
    TBatchInsert.into<T>(connection, tableName, data.ToArray, batchSize);
  end;
end;

function getQuotedIdentifier(identifier: string): string;
begin
  Result := '`' + StringReplace(identifier, '`', '``', [rfReplaceAll]) + '`';
end;

function getQuotedTableName(tableName: string): string;
var
  _parts: TArray<string>;
  _result: string;
  i: integer;
begin
  validateThatStringIsNotEmpty(tableName, 'Table name cannot be empty');

  _parts := tableName.Split(['.']);
  _result := EMPTY_STRING;
  for i := 0 to High(_parts) do
  begin
    if i > 0 then
    begin
      _result := _result + '.';
    end;
    _result := _result + getQuotedIdentifier(_parts[i]);
  end;

  Result := _result;
end;

function isIgnored(member: TRttiMember): boolean;
begin
  Result := member.GetAttribute<IgnoreAttribute> <> nil;
end;

function isPublicMember(member: TRttiMember): boolean;
begin
  Result := member.Visibility in [mvPublic, mvPublished];
end;

function getColumnName(member: TRttiMember): string;
var
  _customName: CustomNameAttribute;
  _result: string;
begin
  _result := member.Name;

  _customName := member.GetAttribute<CustomNameAttribute>;
  if _customName <> nil then
  begin
    _result := _customName.value;
  end;

  Result := _result;
end;

function getMemberInfoFromField(field: TRttiField): TMemberInfo;
var
  _result: TMemberInfo;
begin
  _result := Default(TMemberInfo);
  _result.field := field;
  _result.memberType := field.FieldType;
  _result.columnName := getColumnName(field);

  Result := _result;
end;

function getMemberInfoFromProperty(prop: TRttiProperty): TMemberInfo;
var
  _result: TMemberInfo;
begin
  _result := Default(TMemberInfo);
  _result.prop := prop;
  _result.memberType := prop.PropertyType;
  _result.columnName := getColumnName(prop);

  Result := _result;
end;

//insertable members: record -> fields; class -> public fields + readable public
//properties. [Ignore] is skipped; column name comes from [CustomName] or the name.
function getInsertableMembers(rttiType: TRttiType): TArray<TMemberInfo>;
var
  _members: TList<TMemberInfo>;
  _field: TRttiField;
  _prop: TRttiProperty;
  _isClass: boolean;
  _result: TArray<TMemberInfo>;
begin
  if not (rttiType.TypeKind in [tkRecord, tkClass]) then
  begin
    raise Exception.Create('Type must be a record or class: ' + rttiType.Name);
  end;
  _isClass := rttiType.TypeKind = tkClass;

  _members := TList<TMemberInfo>.Create;
  try
    for _field in rttiType.GetFields do
    begin
      if isIgnored(_field) then
      begin
        Continue;
      end;
      if _isClass and (not isPublicMember(_field)) then
      begin
        Continue;
      end;
      _members.Add(getMemberInfoFromField(_field));
    end;

    if _isClass then
    begin
      for _prop in rttiType.GetProperties do
      begin
        if isIgnored(_prop) then
        begin
          Continue;
        end;
        if not isPublicMember(_prop) then
        begin
          Continue;
        end;
        if not _prop.IsReadable then
        begin
          Continue;
        end;
        _members.Add(getMemberInfoFromProperty(_prop));
      end;
    end;

    _result := _members.ToArray;
  finally
    FreeAndNil(_members);
  end;

  if Length(_result) = 0 then
  begin
    raise Exception.Create('No insertable members found in type: ' + rttiType.Name);
  end;

  Result := _result;
end;

function getColumnDefinitions(rttiType: TRttiType): string;
var
  _members: TArray<TMemberInfo>;
  _member: TMemberInfo;
  _isFirst: boolean;
  _result: string;
begin
  _members := getInsertableMembers(rttiType);

  _result := EMPTY_STRING;
  _isFirst := true;
  for _member in _members do
  begin
    if not _isFirst then
    begin
      _result := _result + ', ';
    end;
    _result := _result + getQuotedIdentifier(_member.columnName) + ' ' +
      mapToMySQLType(_member.memberType);
    _isFirst := false;
  end;

  Result := _result;
end;

function getColumnsCsv(members: TArray<TMemberInfo>): string;
var
  _member: TMemberInfo;
  _isFirst: boolean;
  _result: string;
begin
  _result := EMPTY_STRING;
  _isFirst := true;
  for _member in members do
  begin
    if not _isFirst then
    begin
      _result := _result + ', ';
    end;
    _result := _result + getQuotedIdentifier(_member.columnName);
    _isFirst := false;
  end;

  Result := _result;
end;

function getInstanceOfValue(value: TValue; typeKind: TTypeKind): Pointer;
var
  _result: Pointer;
begin
  if typeKind = tkRecord then
  begin
    _result := value.GetReferenceToRawData;
  end
  else
  begin
    _result := Pointer(value.AsObject);
  end;

  Result := _result;
end;

function getRowAsSQL(instance: Pointer; members: TArray<TMemberInfo>): string;
var
  _member: TMemberInfo;
  _value: TValue;
  _isFirst: boolean;
  _result: string;
begin
  _result := '(';
  _isFirst := true;
  for _member in members do
  begin
    if not _isFirst then
    begin
      _result := _result + ', ';
    end;

    if _member.field <> nil then
    begin
      _value := _member.field.GetValue(instance);
    end
    else
    begin
      _value := _member.prop.GetValue(instance);
    end;

    _result := _result + formatValueForSQL(_value, _member.memberType);
    _isFirst := false;
  end;
  _result := _result + ')';

  Result := _result;
end;

procedure batchInsertValues(connection: TConnection; tableName: string;
  data: TArray<TValue>; typeInfo: PTypeInfo; batchSize: integer = DEFAULT_BATCH_SIZE);
var
  _ctx: TRttiContext;
  _rttiType: TRttiType;
  _members: TArray<TMemberInfo>;
  _insertHead: string;
  _values: TStringBuilder;
  _rowsInBatch: integer;
  _instance: Pointer;
  i: integer;

  procedure flushBatch;
  begin
    if _rowsInBatch > 0 then
    begin
      KLib.MySQL.Utils.executeQuery(_insertHead + _values.ToString, connection);
      _values.Clear;
      _rowsInBatch := 0;
    end;
  end;

begin
  if batchSize < 1 then
  begin
    raise Exception.Create('Batch size must be greater than zero');
  end;

  if Length(data) = 0 then
  begin
    Exit;
  end;

  _rttiType := _ctx.GetType(typeInfo);
  _members := getInsertableMembers(_rttiType);
  _insertHead := 'INSERT INTO ' + getQuotedTableName(tableName) +
    ' (' + getColumnsCsv(_members) + ') VALUES ';

  _values := TStringBuilder.Create;
  try
    _rowsInBatch := 0;

    for i := 0 to High(data) do
    begin
      _instance := getInstanceOfValue(data[i], _rttiType.TypeKind);
      if _instance <> nil then
      begin
        if _rowsInBatch > 0 then
        begin
          _values.Append(', ');
        end;
        _values.Append(getRowAsSQL(_instance, _members));
        Inc(_rowsInBatch);

        if (_rowsInBatch >= batchSize) or (_values.Length >= MAX_BATCH_LENGTH_IN_CHARS) then
        begin
          flushBatch;
        end;
      end;
    end;

    flushBatch;
  finally
    FreeAndNil(_values);
  end;
end;

function isUnsignedInt64Type(typeInfoHandle: PTypeInfo): boolean;
var
  _typeData: PTypeData;
  _result: boolean;
begin
  _result := false;

  if (typeInfoHandle <> nil) and (typeInfoHandle^.Kind = tkInt64) then
  begin
    _typeData := GetTypeData(typeInfoHandle);
    _result := _typeData.MinInt64Value > _typeData.MaxInt64Value;
  end;

  Result := _result;
end;

function mapToMySQLType(rttiType: TRttiType): string;
var
  _result: string;
begin
  case rttiType.TypeKind of
    tkInteger:
      _result := 'BIGINT';

    tkInt64:
      begin
        if isUnsignedInt64Type(rttiType.Handle) then
        begin
          _result := 'BIGINT UNSIGNED';
        end
        else
        begin
          _result := 'BIGINT';
        end;
      end;

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
  else
    _result := 'TEXT';
  end;

  Result := _result;
end;

function getOrdinalAsSQL(value: TValue): string;
var
  _result: string;
begin
  if isUnsignedInt64Type(value.TypeInfo) then
  begin
    _result := UIntToStr(UInt64(value.AsInt64));
  end
  else if value.Kind = tkInt64 then
  begin
    _result := IntToStr(value.AsInt64);
  end
  else
  begin
    _result := IntToStr(value.AsOrdinal);
  end;

  Result := _result;
end;

function getFloatAsSQL(value: TValue; fieldType: TRttiType): string;
var
  _result: string;
begin
  if fieldType.Handle = TypeInfo(TDateTime) then
  begin
    _result := getMySQLSingleQuotedString(
      FormatDateTime('yyyy-mm-dd hh:nn:ss', value.AsType<TDateTime>));
  end
  else if fieldType.Handle = TypeInfo(TDate) then
  begin
    _result := getMySQLSingleQuotedString(
      FormatDateTime('yyyy-mm-dd', value.AsType<TDate>));
  end
  else
  begin
    _result := FloatToStr(value.AsExtended, TFormatSettings.Invariant);
  end;

  Result := _result;
end;

function getVariantAsSQL(value: TValue): string;
var
  _variant: Variant;
  _result: string;
begin
  _variant := value.AsVariant;

  if VarIsNull(_variant) or VarIsEmpty(_variant) then
  begin
    _result := 'NULL';
  end
  else
  begin
    _result := getMySQLSingleQuotedString(VarToStr(_variant));
  end;

  Result := _result;
end;

function formatValueForSQL(value: TValue; fieldType: TRttiType): string;
var
  _result: string;
begin
  if value.IsEmpty then
  begin
    _result := 'NULL';
  end
  else
  begin
    case fieldType.TypeKind of
      tkInteger, tkInt64:
        _result := getOrdinalAsSQL(value);

      tkFloat:
        _result := getFloatAsSQL(value, fieldType);

      tkString, tkLString, tkWString, tkUString:
        _result := getMySQLSingleQuotedString(value.AsString);

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
            _result := getMySQLSingleQuotedString(value.ToString);
          end;
        end;

      tkChar, tkWChar:
        _result := getMySQLSingleQuotedString(value.ToString);

      tkVariant:
        _result := getVariantAsSQL(value);
    else
      _result := getMySQLSingleQuotedString(value.ToString);
    end;
  end;

  Result := _result;
end;

end.
