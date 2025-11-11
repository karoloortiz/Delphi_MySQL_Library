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

    //    function getCreateTemporaryTableStatement<T>(const tableName: string): string;

    class function getSQL<T>(const myRecord: T): string;

    class function processRecord(Instance: Pointer; TypeInfo: PTypeInfo): string;
    class function processClass(ClassInstance: TObject; ClassType: TRttiInstanceType): string;
  public
    tableName: String;
    property isCreated: boolean read _isCreated;

    constructor create(connection: TConnection; tableName: string = EMPTY_STRING; selectQueryStmt: string = EMPTY_STRING);
    procedure recreate(selectQueryStmt: string; tableName: string = EMPTY_STRING; connection: TConnection = nil);
    //    procedure recreate<T>(data: Array of T; tableName: string = EMPTY_STRING; connection: TConnection = nil); overload;
    procedure execute;
    procedure drop;
    destructor Destroy; override;
  end;

function getCreateTemporaryTableFromQuery_SQLStmt(const tableName: string; const queryStmt: string): string;
function getDropTemporaryTable_SQLStmt(const tableName: string): string;

implementation

uses
  System.SysUtils,
  KLib.sqlstring, KLib.Validate, KLib.Utils, KLib.Generics.Attributes,
  KLib.StringUtils,
  KLib.MySQL.Utils;

class function TTemporaryTable.getSQL<T>(const myRecord: T): string;
var
  Ctx: TRttiContext;
  RttiType: TRttiType;
  Value: TValue;
  ObjInstance: TObject;
begin
  RttiType := Ctx.GetType(TypeInfo(T));

  TValue.Make(@myRecord, TypeInfo(T), Value);

  if RttiType.TypeKind = tkClass then
  begin
    ObjInstance := Value.AsObject;
    if ObjInstance = nil then
    begin
      raise Exception.Create('Object nil');
    end
    else
    begin
      Result := processClass(ObjInstance, RttiType as TRttiInstanceType);
    end;
  end
  else if RttiType.TypeKind = tkRecord then
  begin
    Result := processRecord(@myRecord, TypeInfo(T));
  end
  else
  begin
    raise Exception.Create('Type must be a class or record');
  end;
end;

class function TTemporaryTable.processRecord(Instance: Pointer; TypeInfo: PTypeInfo): string;
begin

end;

class function TTemporaryTable.processClass(ClassInstance: TObject; ClassType: TRttiInstanceType): string;
var
  Field: TRttiField;
  Prop: TRttiProperty;
  FieldValue: TValue;
  FieldName: string;
  _customName: string;
  _minAttributeValue: double;
  _maxAttributeValue: double;
  _isRequiredAttribute: boolean;
  _isDefaultAttribute: boolean;
  _errorMessage: string;
  _fieldOffset: Integer;
begin
  //  Result := '';
  //
  //  _isRequiredAttribute := false;
  //  try
  //    if ClassInstance = nil then
  //    begin
  //      Exit;
  //    end;
  //
  //    for Field in ClassType.GetFields do
  //    begin
  //      if Field.Visibility in [mvPublic, mvPublished] then
  //      begin
  //        if Field.GetAttribute<IgnoreAttribute> <> nil then
  //          Continue;
  //
  //        _customName := Field.Name;
  //        if Field.GetAttribute<CustomNameAttribute> <> nil then
  //        begin
  //          _customName := Field.GetAttribute<CustomNameAttribute>.Value;
  //        end;
  //
  //        try
  //          _fieldOffset := Field.Offset;
  //
  //          if (_fieldOffset < 0) or (_fieldOffset > 100000) then
  //          begin
  //            //Result.AddPair(_customName, TJSONNull.Create);   todo
  //            Continue;
  //          end;
  //
  //          try
  //            if Field.FieldType.Name.StartsWith('TDictionary<') then
  //            begin
  //              // todo adds call to KLib.Generics.JSON
  //            end
  //            else
  //            begin
  //              FieldValue := Field.GetValue(ClassInstance);
  //            end;
  //          except
  //            on E: EAccessViolation do
  //            begin
  //              if Field.FieldType.TypeKind = tkClass then
  //              begin
  //                if not AIgnoreEmpty then
  //                  Result.AddPair(_customName, TJSONNull.Create);
  //                Continue;
  //              end
  //              else
  //              begin
  //                try
  //                  FieldValue := getDefaultTValue(Field);
  //                except
  //                  Continue;
  //                end;
  //              end;
  //            end;
  //            on E: Exception do
  //            begin
  //              if Field.FieldType.TypeKind = tkClass then
  //              begin
  //                if not AIgnoreEmpty then
  //                  Result.AddPair(_customName, TJSONNull.Create);
  //                Continue;
  //              end
  //              else
  //                raise;
  //            end;
  //          end;
  //
  //          _isRequiredAttribute := Field.GetAttribute<RequiredAttribute> <> nil;
  //          _isDefaultAttribute := Field.GetAttribute<DefaultValueAttribute> <> nil;
  //
  //          if (Field.FieldType.TypeKind = tkClass) then
  //          begin
  //            if FieldValue.IsEmpty or (FieldValue.AsObject = nil) then
  //            begin
  //              if _isRequiredAttribute and not _isDefaultAttribute then
  //              begin
  //                raise Exception.Create('Field required: ' + _customName);
  //              end;
  //              if AIgnoreEmpty then
  //                Continue
  //              else
  //                Result.AddPair(_customName, TJSONNull.Create);
  //              Continue;
  //            end;
  //          end;
  //
  //          if (checkIfTValueIsEmpty(FieldValue)) and (Field.FieldType.TypeKind <> tkClass) then
  //          begin
  //            if (_isRequiredAttribute) and (not _isDefaultAttribute) then
  //            begin
  //              raise Exception.Create('Field required: ' + _customName);
  //            end;
  //            FieldValue := getDefaultTValue(Field);
  //          end;
  //
  //          // Apply min/max attributes
  //          _minAttributeValue := -1;
  //          if (Field.GetAttribute<MinAttribute> <> nil) then
  //            _minAttributeValue := Field.GetAttribute<MinAttribute>.Value;
  //
  //          _maxAttributeValue := -1;
  //          if (Field.GetAttribute<MaxAttribute> <> nil) then
  //            _maxAttributeValue := Field.GetAttribute<MaxAttribute>.Value;
  //
  //          // Skip empty values if needed
  //          if (AIgnoreEmpty and checkIfTValueIsEmpty(FieldValue)
  //            and (Field.FieldType.TypeKind <> tkRecord)
  //            and (Field.FieldType.TypeKind <> tkClass)) then
  //            Continue;
  //
  //          JSONElement := getJSONFromTValue(FieldValue, AIgnoreEmpty, _minAttributeValue, _maxAttributeValue);
  //
  //          if (JSONElement <> nil) then
  //          begin
  //            Result.AddPair(_customName, JSONElement);
  //          end;
  //        except
  //          on E: EAccessViolation do
  //          begin
  //            if not AIgnoreEmpty then
  //              Result.AddPair(_customName, TJSONNull.Create);
  //          end;
  //          on E: Exception do
  //          begin
  //            _errorMessage := Format('JSON process error in field "%s": %s',
  //              [Field.Name, E.Message]);
  //            raise Exception.Create(_errorMessage);
  //          end;
  //        end;
  //      end;
  //    end;
  //
  //    for Prop in ClassType.GetProperties do
  //    begin
  //      if (Prop.Visibility in [mvPublic, mvPublished]) and Prop.IsReadable then
  //      begin
  //        if Prop.GetAttribute<IgnoreAttribute> <> nil then
  //          Continue;
  //
  //        _customName := Prop.Name;
  //        if Prop.GetAttribute<CustomNameAttribute> <> nil then
  //          _customName := Prop.GetAttribute<CustomNameAttribute>.Value;
  //
  //        if Result.FindValue(_customName) <> nil then
  //          Continue;
  //
  //        try
  //          try
  //            FieldValue := Prop.GetValue(ClassInstance);
  //          except
  //            on E: EAccessViolation do
  //            begin
  //              if Prop.PropertyType.TypeKind = tkClass then
  //              begin
  //                if not AIgnoreEmpty then
  //                  Result.AddPair(_customName, TJSONNull.Create);
  //              end;
  //              Continue;
  //            end;
  //            on E: Exception do
  //            begin
  //              Continue;
  //            end;
  //          end;
  //
  //          _isRequiredAttribute := Prop.GetAttribute<RequiredAttribute> <> nil;
  //          _isDefaultAttribute := Prop.GetAttribute<DefaultValueAttribute> <> nil;
  //
  //          if (Prop.PropertyType.TypeKind = tkClass) then
  //          begin
  //            if FieldValue.IsEmpty or (FieldValue.AsObject = nil) then
  //            begin
  //              if _isRequiredAttribute and not _isDefaultAttribute then
  //                raise Exception.Create('Property required: ' + _customName);
  //
  //              if AIgnoreEmpty then
  //                Continue
  //              else
  //                Result.AddPair(_customName, TJSONNull.Create);
  //              Continue;
  //            end;
  //          end;
  //
  //          if (checkIfTValueIsEmpty(FieldValue)) and (Prop.PropertyType.TypeKind <> tkClass) then
  //          begin
  //            if (_isRequiredAttribute) and (not _isDefaultAttribute) then
  //              raise Exception.Create('Property required: ' + _customName);
  //            if not _isRequiredAttribute then
  //              Continue;
  //          end;
  //
  //          _minAttributeValue := -1;
  //          if (Prop.GetAttribute<MinAttribute> <> nil) then
  //            _minAttributeValue := Prop.GetAttribute<MinAttribute>.Value;
  //
  //          _maxAttributeValue := -1;
  //          if (Prop.GetAttribute<MaxAttribute> <> nil) then
  //            _maxAttributeValue := Prop.GetAttribute<MaxAttribute>.Value;
  //
  //          if (AIgnoreEmpty and checkIfTValueIsEmpty(FieldValue)
  //            and (Prop.PropertyType.TypeKind <> tkClass)) then
  //            Continue;
  //
  //          JSONElement := getJSONFromTValue(FieldValue, AIgnoreEmpty, _minAttributeValue, _maxAttributeValue);
  //
  //          if (JSONElement <> nil) then
  //            Result.AddPair(_customName, JSONElement);
  //        except
  //          on E: Exception do
  //          begin
  //            if _isRequiredAttribute then
  //            begin
  //              _errorMessage := Format('JSON process error in property "%s": %s',
  //                [Prop.Name, E.Message]);
  //              raise Exception.Create(_errorMessage);
  //            end;
  //            Continue;
  //          end;
  //        end;
  //      end;
  //    end;
  //  except
  //    on E: Exception do
  //    begin
  //      Result.Free;
  //      raise;
  //    end;
  //  end;
end;

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

destructor TTemporaryTable.destroy;
begin
  drop;
  inherited;
end;

end.
