unit KLib.MySQL.TemporaryTable;

interface

uses
  KLib.MySQL.DriverPort;

type
  TTemporaryTable = class
  private
    _connection: TConnection;
    _selectQueryStmt: string;
    _status: boolean;
  public
    tableName: String;
    property isCreated: boolean read _status;

    constructor create(tableName: string; selectQueryStmt: string; connection: TConnection);
    procedure execute;
    procedure drop;
    destructor Destroy; override;
  end;

function getCreateTemporaryTableFromQuery_SQLStmt(tableName: string; queryStmt: string): string;
function getDropTemporaryTable_SQLStmt(tableName: string): string;

implementation

uses
  KLib.MySQL.Utils,
  KLib.MyString,
  System.SysUtils;

//todo MOVE IN KLIB.MYSQL.CONSTANTS?
function getCreateTemporaryTableFromQuery_SQLStmt(tableName: string; queryStmt: string): string;
const
  PARAM_TABLENAME = ':TABLENAME';
  PARAM_QUERYSTMT = ':QUERYSTMT';
  CREATE_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME_PARAM_QUERYSTMT =
    'CREATE TEMPORARY TABLE' + sLineBreak +
    PARAM_TABLENAME + sLineBreak +
    PARAM_QUERYSTMT;
var
  _queryStmt: myString;
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
  _queryStmt: myString;
begin
  _queryStmt := DROP_TEMPORANY_TABLE_WHERE_PARAM_TABLENAME;
  _queryStmt.setParamAsString(PARAM_TABLENAME, tableName);

  Result := _queryStmt;
end;

constructor TTemporaryTable.create(tableName: string; selectQueryStmt: string; connection: TConnection);
begin
  self.tableName := tableName;
  self._selectQueryStmt := selectQueryStmt;
  self._connection := connection;
  _status := false;
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

  _status := true;
end;

procedure TTemporaryTable.drop;
var
  _queryStmt: string;
begin
  if isCreated then
  begin
    _queryStmt := getDropTemporaryTable_SQLStmt(tableName);
    executeQuery(_queryStmt, _connection);
    _status := false;
  end;
end;

destructor TTemporaryTable.destroy;
begin
  drop;
  inherited;
end;

end.
