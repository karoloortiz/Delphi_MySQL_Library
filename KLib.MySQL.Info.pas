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

unit KLib.MySQL.Info;

interface

uses
  KLib.MySQL.Credentials;

type
  TMySQLVersion = (v5_5, v5_7, v_8);

  TMySQLInfo = record
  private
    _credentials: TCredentials;
    _path_bin: string;
    _path_ini: string;
    function getCredentials: TCredentials;
    procedure setCredentials(value: TCredentials);
    function getPortIniFile: integer;
    procedure setPortIniFile(value: integer);
    function get_path_datadirIniFile: string;
    procedure set_path_datadirIniFile(value: string);
    procedure set_path_bin(value: string);
    procedure set_path_ini(value: string);
    function get_path_mysql: string;
    function get_path_mysqladmin: string;
    function get_path_mysqld: string;
    function get_path_mysqldump: string;
    function get_path_mysqlpump: string;
  public
    version: TMySQLVersion;
    path_secure_file_priv: string;
    property credentials: TCredentials read getCredentials write setCredentials;
    property portIniFile: integer read getPortIniFile write setPortIniFile;
    property path_datadirIniFile: string read get_path_datadirIniFile write set_path_datadirIniFile;
    property path_bin: string read _path_bin write set_path_bin;
    property path_ini: string read _path_ini write set_path_ini;
    property path_mysql: string read get_path_mysql;
    property path_mysqladmin: string read get_path_mysqladmin;
    property path_mysqld: string read get_path_mysqld;
    property path_mysqldump: string read get_path_mysqldump;
    property path_mysqlpump: string read get_path_mysqlpump;
  end;

implementation

uses
  KLib.MySQL.IniManipulator, KLib.MySQL.Utils, KLib.MySQL.Validate,
  KLib.Utils, KLib.Validate,
  System.SysUtils;

const
  MYSQL_FILENAME = 'mysql.exe';
  MYSQLADMIN_FILENAME = 'mysqladmin.exe';
  MYSQLD_FILENAME = 'mysqld.exe';
  MYSQLDUMP_FILENAME = 'mysqldump.exe';
  MYSQLPUMP_FILENAME = 'mysqlpump.exe';

function TMySQLInfo.getCredentials: TCredentials;
begin
  try
    _credentials.port := portIniFile;
  except
    on E: Exception do
  end;
  validateRequiredMySQLProperties(_credentials);
  Result := _credentials;
end;

procedure TMySQLInfo.setCredentials(value: TCredentials);
begin
  _credentials := value;
  if _credentials.server = '' then
  begin
    _credentials.server := DEFAULT_MYSQL_CREDENTIALS.server;
  end;
  if _credentials.credentials.username = '' then
  begin
    _credentials.credentials.username := DEFAULT_MYSQL_CREDENTIALS.credentials.username;
  end;
  if _credentials.credentials.password = '' then
  begin
    _credentials.credentials.password := DEFAULT_MYSQL_CREDENTIALS.credentials.password;
  end;
end;

procedure TMySQLInfo.set_path_bin(value: string);
begin
  self._path_bin := getValidFullPath(value);
end;

procedure TMySQLInfo.set_path_ini(value: string);
begin
  self._path_ini := getValidFullPath(value);
end;

function TMySQLInfo.get_path_mysql: string;
begin
  Result := getCombinedPath(path_bin, MYSQL_FILENAME);
end;

function TMySQLInfo.get_path_mysqladmin: string;
begin
  Result := getCombinedPath(path_bin, MYSQLADMIN_FILENAME);
end;

function TMySQLInfo.get_path_mysqld: string;
begin
  Result := getCombinedPath(path_bin, MYSQLD_FILENAME);
end;

function TMySQLInfo.get_path_mysqldump: string;
begin
  Result := getCombinedPath(path_bin, MYSQLDUMP_FILENAME);
end;

function TMySQLInfo.get_path_mysqlpump: string;
begin
  Result := getCombinedPath(path_bin, MYSQLPUMP_FILENAME);
end;

const
  ERR_MSG_INI_FILE_NOT_SPECIFIED = 'MySQL Ini configuration file were not being specified.';

function TMySQLInfo.getPortIniFile: integer;
var
  _iniManipulator: TMySQLIniManipulator;
  _port: integer;
begin
  validateThatStringIsNotEmpty(path_ini, ERR_MSG_INI_FILE_NOT_SPECIFIED);
  _iniManipulator := TMySQLIniManipulator.Create(path_ini);
  _port := _iniManipulator.port;
  FreeAndNil(_iniManipulator);
  Result := _port;
end;

procedure TMySQLInfo.setPortIniFile(value: integer);
var
  _iniManipulator: TMySQLIniManipulator;
  _tempCredentials: TCredentials;
begin
  validateThatStringIsNotEmpty(path_ini, ERR_MSG_INI_FILE_NOT_SPECIFIED);
  _iniManipulator := TMySQLIniManipulator.Create(path_ini);
  _iniManipulator.port := value;
  FreeAndNil(_iniManipulator);

  _tempCredentials := _credentials;
  _tempCredentials.port := value;
  credentials := _tempCredentials;
end;

function TMySQLInfo.get_path_datadirIniFile: string;
var
  _iniManipulator: TMySQLIniManipulator;
  _datadir: string;
begin
  validateThatStringIsNotEmpty(path_ini, ERR_MSG_INI_FILE_NOT_SPECIFIED);
  _iniManipulator := TMySQLIniManipulator.Create(path_ini);
  _datadir := _iniManipulator.datadir;
  FreeAndNil(_iniManipulator);
  Result := _datadir;
end;

procedure TMySQLInfo.set_path_datadirIniFile(value: string);
var
  _iniManipulator: TMySQLIniManipulator;
  _datadir: string;
begin
  validateThatStringIsNotEmpty(path_ini, ERR_MSG_INI_FILE_NOT_SPECIFIED);
  _iniManipulator := TMySQLIniManipulator.Create(path_ini);
  _datadir := getValidFullPath(value);
  _iniManipulator.datadir := _datadir;
  FreeAndNil(_iniManipulator);
end;

end.
