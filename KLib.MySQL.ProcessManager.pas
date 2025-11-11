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

unit KLib.MySQL.ProcessManager;

interface

uses
  KLib.MySQL.Driver, KLib.MySQL.Process, KLib.MySQL.Info, KLib.VC_Redist,
  KLib.Types, KLib.Constants;

type
  TMySQLProcessManager = class
  private
    allPersonalConnectionsAreClosed: boolean;
    numberActiveConnections: integer;
    mySQLProcess: TMySQLProcess;
    autoGetFirstPortAvaliable: boolean;

    function getPort: integer;
    procedure setPort(value: integer);

    procedure startMySQLProcess;
    function canYouShutdown: boolean;
    function canYouShutdown_personalConnectionsClosed: boolean;
    function canYouShutdown_personalConnectionsActived: boolean;
    property isShutdownEnabled: boolean read canYouShutdown;
  public
    connection: TConnection;
    errMsg_startFailed: string;
    confirmMsg_forceShutdown: string;
    property port: integer read getPort write setPort;
    constructor create(info: TMySQLInfo; numberActiveConnections: integer = 1; allPersonalConnectionsAreClosed: boolean = true;
      errMsg_startFailed: string = 'MySQL not started.'; confirmMsg_forceShutdown: string = 'Other programs are connected to the database, force MySQL shutdown?.');
    procedure AStart(reply: TAsyncifyMethodReply; autoGetFirstPortAvaliable: boolean = true); overload;
    procedure AStart(callBacks: TCallbacks; autoGetFirstPortAvaliable: boolean = true); overload;
    procedure AStart(_then: TCallBack; _catch: TCallback; autoGetFirstPortAvaliable: boolean = true); overload;
    procedure start(VC_RedistInstallOpts: TVC_RedistInstallOpts; autoGetFirstPortAvaliable: boolean = true); overload;
    procedure start(autoGetFirstPortAvaliable: boolean = true); overload;
    procedure shutdown(force: boolean = NOT_FORCE);
    destructor Destroy; override;
  end;

implementation

uses
  KLib.MySQL.Utils,
  KLib.Asyncify, KLib.AsyncMethod,
  KLib.sqlstring,
  Vcl.Dialogs, Vcl.Controls,
  System.SysUtils, System.UITypes;

constructor TMySQLProcessManager.create(info: TMySQLInfo; numberActiveConnections: integer = 1; allPersonalConnectionsAreClosed: boolean = true;
  errMsg_startFailed: string = 'MySQL not started.'; confirmMsg_forceShutdown: string = 'Other programs are connected to the database, force MySQL shutdown?.');
begin
  Self.mySQLProcess := TMySQLProcess.create(info);
  Self.connection := getTConnection(mySQLProcess.info.credentials);

  Self.numberActiveConnections := numberActiveConnections;
  Self.allPersonalConnectionsAreClosed := allPersonalConnectionsAreClosed;
  Self.errMsg_startFailed := errMsg_startFailed;
  Self.confirmMsg_forceShutdown := confirmMsg_forceShutdown;
end;

procedure TMySQLProcessManager.AStart(reply: TAsyncifyMethodReply; autoGetFirstPortAvaliable: boolean = true);
begin
  Self.autoGetFirstPortAvaliable := autoGetFirstPortAvaliable;
  asyncify(startMySQLProcess, reply);
end;

procedure TMySQLProcessManager.AStart(callBacks: TCallbacks; autoGetFirstPortAvaliable: boolean = true);
begin
  AStart(TCallBack(callBacks.resolve), TCallback(callBacks.reject), autoGetFirstPortAvaliable);
end;

procedure TMySQLProcessManager.AStart(_then: TCallBack; _catch: TCallback; autoGetFirstPortAvaliable: boolean = true);
const
  DEFAULT_RESOLVE_MSG_MYSQL_STARTED = 'MySQL started.';
begin
  Self.autoGetFirstPortAvaliable := autoGetFirstPortAvaliable;
  TAsyncMethod.Create(
    procedure(resolve: TCallBack; reject: TCallback)
    begin
      startMySQLProcess;
      resolve(DEFAULT_RESOLVE_MSG_MYSQL_STARTED);
    end,
    procedure(value: String)
    begin
      _then(value);
    end,
    procedure(value: String)
    begin
      _catch(value);
    end);
end;

procedure TMySQLProcessManager.start(VC_RedistInstallOpts: TVC_RedistInstallOpts;
autoGetFirstPortAvaliable: boolean = true);
begin
  installVC_RedistIfNotExists(VC_RedistInstallOpts);
  start(autoGetFirstPortAvaliable);
end;

procedure TMySQLProcessManager.start(autoGetFirstPortAvaliable: boolean = true);
begin
  Self.autoGetFirstPortAvaliable := autoGetFirstPortAvaliable;
  startMySQLProcess;
end;

procedure TMySQLProcessManager.startMySQLProcess;
begin
  if not mySQLProcess.isStarted then
  begin
    try
      mySQLProcess.start(autoGetFirstPortAvaliable);
      connection.Port := port;
      connection.Connected := true;
    except
      on E: Exception do
      begin
        raise Exception.Create(errMsg_startFailed);
      end;
    end;
  end;
end;

procedure TMySQLProcessManager.shutdown(force: boolean = NOT_FORCE);
begin
  if mySQLProcess.isStarted then
  begin
    if (isShutdownEnabled) or (force) then
    begin
      mySQLProcess.stop(force);
    end;
  end;
end;

function TMySQLProcessManager.canYouShutdown: boolean;
begin
  if allPersonalConnectionsAreClosed then
  begin
    result := canYouShutdown_personalConnectionsClosed;
  end
  else
  begin
    result := canYouShutdown_personalConnectionsActived;
  end;
end;

function TMySQLProcessManager.canYouShutdown_personalConnectionsClosed: boolean;
const
  SELECT_USER =
    'SELECT' + sLineBreak +
    'USER' + sLineBreak +
    'FROM' + sLineBreak +
    'information_schema.PROCESSLIST' + sLineBreak +
    'WHERE' + sLineBreak +
    'USER <> (IF(@@event_scheduler = "ON", "event_scheduler", ""))';
var
  _result: boolean;
  _query: TQuery;
  _realNumberConnections: integer;
begin
  _result := false;

  _query := getTQuery(connection, SELECT_USER);
  _query.Open;
  _realNumberConnections := _query.RecordCount - 1;
  if _realNumberConnections = 0 then
  begin
    _result := true;
  end
  else
  begin
    if (_realNumberConnections > 0) and (_realNumberConnections < numberActiveConnections) then
    begin
      if messagedlg(confirmMsg_forceShutdown, mtCustom, [mbYes, mbCancel], 0) = mrYes then
      begin
        _result := true;
      end;
    end
    else
    begin
      _result := false;
    end;
  end;
  FreeAndNil(_query);
  Result := _result;
end;

function TMySQLProcessManager.canYouShutdown_personalConnectionsActived: boolean;
const
  PARAM_USER = ':USER';
  PARAM_HAVING_CONDITION = ':HAVING_CONDITION';

  SELECT_USER_WHERE_PARAM_USER_PARAM_HAVING_CONDITION =
    'SELECT' + sLineBreak +
    'USER' + sLineBreak +
    'FROM' + sLineBreak +
    'information_schema.PROCESSLIST' + sLineBreak +
    'WHERE' + sLineBreak +
    'USER <> (IF(@@event_scheduler = "ON", "event_scheduler", ""))' + sLineBreak +
    'AND USER = ' + PARAM_USER + sLineBreak +
    'GROUP BY' + sLineBreak +
    'USER' + sLineBreak +
    'HAVING' + sLineBreak +
    'COUNT(USER) ' + PARAM_HAVING_CONDITION + sLineBreak +
    '' + sLineBreak +
    'UNION ALL' + sLineBreak +
    '' + sLineBreak +
    'SELECT' + sLineBreak +
    'USER' + sLineBreak +
    'FROM' + sLineBreak +
    'information_schema.PROCESSLIST' + sLineBreak +
    'WHERE' + sLineBreak +
    'USER <> (IF(@@event_scheduler = "ON", "event_scheduler", ""))' + sLineBreak +
    'AND USER <> ' + PARAM_USER + sLineBreak +
    'AND EXISTS (' + sLineBreak +
    '	SELECT' + sLineBreak +
    '	USER' + sLineBreak +
    '	FROM' + sLineBreak +
    '	information_schema.PROCESSLIST' + sLineBreak +
    '	WHERE' + sLineBreak +
    '	USER <> (IF(@@event_scheduler = "ON", "event_scheduler", ""))' + sLineBreak +
    '	AND USER = ' + PARAM_USER + sLineBreak +
    '	GROUP BY' + sLineBreak +
    '	USER' + sLineBreak +
    '	HAVING' + sLineBreak +
    '	COUNT(USER) = 1' + sLineBreak +
    ')' + sLineBreak +
    'GROUP BY' + sLineBreak +
    'USER';
var
  _result: boolean;
  _query: TQuery;
  _queryStmt: sqlstring;
  _user: string;
  _realNumberConnections: integer;

  _havingCondition: string;
begin
  _user := mySQLProcess.info.credentials.credentials.username;
  _realNumberConnections := numberActiveConnections + 1;
  if numberActiveConnections > 1 then
  begin
    _havingCondition := 'BETWEEN ' + IntToStr(_realNumberConnections - 1) + ' AND ' + IntToStr(_realNumberConnections);
  end
  else
  begin
    _havingCondition := '> ' + IntToStr(_realNumberConnections);
  end;

  _queryStmt := SELECT_USER_WHERE_PARAM_USER_PARAM_HAVING_CONDITION;
  _queryStmt.setParamAsDoubleQuotedString(PARAM_USER, _user);
  _queryStmt.setParamAsString(PARAM_HAVING_CONDITION, _havingCondition);

  _query := getTQuery(connection, _queryStmt);

  _query.Open;
  if _query.RecordCount = 0 then
  begin
    _result := true;
  end
  else
  begin
    _result := false;
    if messagedlg(confirmMsg_forceShutdown, mtCustom, [mbYes, mbCancel], 0) = mrYes then
    begin
      _result := true;
    end;
  end;
  FreeAndNil(_query);

  Result := _result;
end;

procedure TMySQLProcessManager.setPort(value: integer);
begin
  mySQLProcess.port := value;
end;

function TMySQLProcessManager.getPort: integer;
begin
  result := mySQLProcess.port;
end;

destructor TMySQLProcessManager.Destroy;
begin
  if Assigned(mySQLProcess) then
  begin
    shutdown;
    FreeAndNil(mySQLProcess);
    FreeAndNil(connection);
  end;
  inherited;
end;

end.
