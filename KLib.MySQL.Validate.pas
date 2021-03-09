unit KLib.MySQL.Validate;

interface

uses
  KLib.MySQL.Info;

procedure validateThatMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials; errMsg: string = 'The MySQL version is not 8.0 .');
procedure validateThatMysqlVersionIsNot_v_8(mySQLCredentials: TMySQLCredentials; errMsg: string = 'The MySQL version is 8.0 .');
procedure validateMySQLCredentials(mySQLCredentials: TMySQLCredentials; errMsg: string = 'Invalid MySQL credentials.');
procedure validateRequiredMySQLProperties(mySQLCredentials: TMySQLCredentials; errMsg: string = 'MySQL credentials were not fully specified.');

procedure validateThatVC_Redist2013IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 not correctly installed.');
procedure validateThatVC_Redist2013X86IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 x86 not correctly installed.');
procedure validateThatVC_Redist2013X64IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 x64 not correctly installed.');
procedure validateThatVC_Redist2019X64IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2019 x64 not correctly installed.');

implementation

uses
  KLib.MySQL.Utils, KLib.VC_Redist,
  System.SysUtils;

procedure validateThatMysqlVersionIs_v_8(mySQLCredentials: TMySQLCredentials; errMsg: string = 'The MySQL version is not 8.0 .');
begin
  if not checkIfMysqlVersionIs_v_8(mySQLCredentials) then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateThatMysqlVersionIsNot_v_8(mySQLCredentials: TMySQLCredentials; errMsg: string = 'The MySQL version is 8.0 .');
begin
  if checkIfMysqlVersionIs_v_8(mySQLCredentials) then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateMySQLCredentials(mySQLCredentials: TMySQLCredentials; errMsg: string = 'Invalid MySQL credentials.');
begin
  if not checkMySQLCredentials(mySQLCredentials) then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateRequiredMySQLProperties(mySQLCredentials: TMySQLCredentials; errMsg: string = 'MySQL credentials were not fully specified.');
begin
  if not checkRequiredMySQLProperties(mySQLCredentials) then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateThatVC_Redist2013IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 not correctly installed.');
begin
  if not checkIfVC_Redist2013IsInstalled then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateThatVC_Redist2013X86IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 x86 not correctly installed.');
begin
  if not checkIfVC_Redist2013X86IsInstalled then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateThatVC_Redist2013X64IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2013 x64 not correctly installed.');
begin
  if not checkIfVC_Redist2013X64IsInstalled then
  begin
    raise Exception.Create(errMsg);
  end;
end;

procedure validateThatVC_Redist2019X64IsInstalled(errMsg: string = 'Microsoft Visual C++ Redistributable 2019 x64 not correctly installed.');
begin
  if not checkIfVC_Redist2019X64IsInstalled then
  begin
    raise Exception.Create(errMsg);
  end;
end;

end.
