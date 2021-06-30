{
  KLib Version = 1.0
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
