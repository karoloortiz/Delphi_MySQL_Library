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
  System.SysUtils;

//todo refactor create query unit and use klib.mystring
function getCreateTemporaryTableFromQuery_SQLStmt(tableName: string; queryStmt: string): string;
begin
  Result := 'CREATE TEMPORARY TABLE ' + tableName + sLineBreak +
    queryStmt;
end;

function getDropTemporaryTable_SQLStmt(tableName: string): string;
const
  DROP_TEMPORANY_TABLE_QUERY_STMT = 'DROP TEMPORARY TABLE ';
begin
  Result := DROP_TEMPORANY_TABLE_QUERY_STMT + tableName;
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
