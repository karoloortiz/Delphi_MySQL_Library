{
  KLib Version = 3.0
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

unit KLib.MySQL.Process;

interface

uses
  KLib.MySQL.Info, KLib.VC_Redist,
  KLib.Types;

type

  TMySQLProcess = class
  private
    function checkIfMySQLIsStarted: boolean;
    function getMySQLCredentials: TMySQLCredentials;
    function getCredentials: TCredentials;
    procedure setCredentials(value: TCredentials);
    function getPort: integer;
    procedure setPort(value: integer);
    procedure setFirstPortAvaliable(enable: boolean);
    procedure waitUntilProcessStart;
  public
    info: TMySQLInfo;
    property isStarted: boolean read checkIfMySQLIsStarted;
    property mySQLCredentials: TMySQLCredentials read getMySQLCredentials;
    property credentials: TCredentials read getCredentials write setCredentials;
    property port: integer read getPort write setPort;
    constructor create(mySQLInfo: TMySQLInfo);
    procedure start(autoGetFirstPortAvaliable: boolean = true);
    procedure stop;
    destructor Destroy; override;
  end;

implementation

uses
  KLib.MySQL.Utils, KLib.MySQL.CLIUtilities, KLib.MySQL.Validate,
  KLib.Utils, KLib.Constants, KLib.Validate, KLib.Windows,
  Vcl.Dialogs, Vcl.Controls,
  System.SysUtils, System.UITypes;

constructor TMySQLProcess.create(mySQLInfo: TMySQLInfo);
const
  ERR_MSG = 'MySQL version were not being specified.';
var
  _tempCredentials: TMySQLCredentials;
begin
  Self.info := mySQLInfo;

  validateThatFileExists(Self.info.path_ini);
  validateThatFileExists(Self.info.path_mysqld);
  validateThatDirExists(Self.info.path_datadirIniFile);

  case Self.info.version of
    TMySQLVersion.v5_7:
      ;
    TMySQLVersion.v_8:
      ;
  else
    raise Exception.Create(ERR_MSG);
  end;

  _tempCredentials := Self.info.credentials;
  with _tempCredentials do
  begin
    server := LOCALHOST_IP_ADDRESS;
    database := EMPTY_STRING;
  end;
  Self.info.credentials := _tempCredentials;
end;

procedure TMySQLProcess.start(autoGetFirstPortAvaliable: boolean = true);
const
  ERR_MSG_VC_REDIST_NOT_INSTALLED = 'Microsoft Visual C++ Redistributable not installed.';
  SHOW_WINDOW_HIDE = _SW_HIDE;
  RAISE_EXCEPTION_IF_FUNCTION_FAILS = true;

  NOT_ACCORDING_WINDOWS_ARCHITECTURE = false;
var
  _mysqldParams: string;
  _doubleQuotedIniPath: string;
  _isVC_RedistInstalled: boolean;
begin
  if not isStarted then
  begin
    _isVC_RedistInstalled := false;

    case Self.info.version of
      TMySQLVersion.v5_7:
        _isVC_RedistInstalled := checkIfVC_Redist2013IsInstalled(NOT_ACCORDING_WINDOWS_ARCHITECTURE);
      TMySQLVersion.v_8:
        _isVC_RedistInstalled := checkIfVC_Redist2019X64IsInstalled;
    end;

    if not _isVC_RedistInstalled then
    begin
      raise Exception.Create(ERR_MSG_VC_REDIST_NOT_INSTALLED);
    end;

    setFirstPortAvaliable(autoGetFirstPortAvaliable);

    _doubleQuotedIniPath := getDoubleQuotedString(info.path_ini);
    _mysqldParams := ' --defaults-file=' + _doubleQuotedIniPath;
    shellExecuteExe(info.path_mysqld, _mysqldParams, SHOW_WINDOW_HIDE, RAISE_EXCEPTION_IF_FUNCTION_FAILS);
    waitUntilProcessStart;
  end;
end;

procedure TMySQLProcess.setFirstPortAvaliable(enable: boolean);
begin
  if enable then
  begin
    port := getFirstPortAvaliable(port);
  end;
end;

procedure TMySQLProcess.waitUntilProcessStart;
const
  MSG_WAIT = 'Apparentely MySQL takes long time to start, would you wait?.';
  ERR_MSG = 'MySQL not started.';
var
  i: integer;
  _exit: boolean;
begin
  i := 0;
  _exit := false;
  while not _exit do
  begin
    if (i > 10) then
    begin
      if messagedlg(MSG_WAIT, mtCustom, [mbYes, mbCancel], 0) = mrYes then
      begin
        i := 0;
      end
      else
      begin
        _exit := true;
      end;
    end;
    try
      validateMySQLCredentials(info.credentials);
      _exit := true;
    except
      Inc(i, 1);
      sleep(3000);
    end;
  end;

  if not isStarted then
  begin
    raise Exception.Create(ERR_MSG);
  end;
end;

procedure TMySQLProcess.stop;
begin
  if isStarted then
  begin
    mysqladminShutdown(info.path_mysqladmin, info.credentials);
  end;
end;

function TMySQLProcess.checkIfMySQLIsStarted: boolean;
var
  _result: boolean;
begin
  _result := checkMySQLCredentials(mySQLCredentials);
  Result := _result;
end;

function TMySQLProcess.getMySQLCredentials: TMySQLCredentials;
var
  _mySQLCredentials: TMySQLCredentials;
begin
  with _mySQLCredentials do
  begin
    credentials := Self.credentials;
    port := Self.port;
    server := LOCALHOST_IP_ADDRESS;
    database := EMPTY_STRING;
  end;
  Result := _mySQLCredentials;
end;

function TMySQLProcess.getCredentials: TCredentials;
begin
  Result := info.credentials.credentials;
end;

procedure TMySQLProcess.setCredentials(value: TCredentials);
var
  _tempCredentials: TMySQLCredentials;
begin
  _tempCredentials := info.credentials;
  _tempCredentials.credentials := value;
  info.credentials := _tempCredentials;
end;

procedure TMySQLProcess.setPort(value: integer);
begin
  info.portIniFile := value;
end;

function TMySQLProcess.getPort: integer;
begin
  result := info.credentials.port;
end;

destructor TMySQLProcess.Destroy;
begin
  inherited;
end;

end.
