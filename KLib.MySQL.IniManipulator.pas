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

unit KLib.MySQL.IniManipulator;

interface

uses
  System.IniFiles;

type
  TMySQLIniManipulator = class(TIniFile)
  private
    function getPort: integer;
    procedure setPort(value: integer);
    function get_loose_keyring_file_data: string;
    procedure set_loose_keyring_file_data(value: string);
    function get_secure_file_priv: string;
    procedure set_secure_file_priv(value: string);
    function getDatadir: string;
    procedure setDatadir(value: string);
    function get_innodb_buffer_pool_size: string;
    procedure set_innodb_buffer_pool_size(value: string);
    function get_innodb_log_file_size: string;
    procedure set_innodb_log_file_size(value: string);
  public
    constructor Create(fileName: string); overload;
    property port: integer read getPort write setPort;
    property loose_keyring_file_data: string read get_loose_keyring_file_data write set_loose_keyring_file_data;
    property secure_file_priv: string read get_secure_file_priv write set_secure_file_priv;
    property datadir: string read getDatadir write setDatadir;
    property innodb_buffer_pool_size: string read get_innodb_buffer_pool_size write set_innodb_buffer_pool_size;
    property innodb_log_file_size: string read get_innodb_log_file_size write set_innodb_log_file_size;
    procedure setOptimizedInnodbSettings(raiseExceptionIfMemoryIsInsufficient: boolean = false);
    procedure setRequiredPathsInIni(path_datadir: string; path_secure_file_priv: string); overload;
    procedure setRequiredPathsInIni(path_datadir: string); overload;
    //TODO ADD procedure TUTF8NoBOMEncoding
  end;

implementation

uses
  KLib.Utils, KLib.MemoryRAM, KLib.Windows, KLib.Validate,
  System.IOUtils, System.SysUtils;

const
  CLIENT_SECTION_NAME = 'client';
  MYSQLD_SECTION_NAME = 'mysqld';
  PORT_PROPERTY_NAME = 'port';
  LOOSE_KEYRING_FILE_DATA_PROPERTY_NAME = 'loose_keyring_file_data';
  SECURE_FILE_PRIV_PROPERTY_NAME = 'secure_file_priv';
  DATADIR_PROPERTY_NAME = 'datadir';
  INNODB_BUFFER_POOL_SIZE_PROPERTY_NAME = 'innodb_buffer_pool_size';
  INNODB_LOG_FILE_SIZE_PROPERTY_NAME = 'innodb_log_file_size';

constructor TMySQLIniManipulator.Create(fileName: string);
var
  _pathFile: string;
begin
  _pathFile := getValidFullPath(fileName);
  validateThatFileExists(_pathFile);

  inherited Create(_pathFile);
end;

function TMySQLIniManipulator.getPort: integer;
begin
  result := ReadInteger(MYSQLD_SECTION_NAME, PORT_PROPERTY_NAME, 0);
end;

procedure TMySQLIniManipulator.setPort(value: integer);
begin
  WriteInteger(MYSQLD_SECTION_NAME, PORT_PROPERTY_NAME, value);
  WriteInteger(CLIENT_SECTION_NAME, PORT_PROPERTY_NAME, value);
end;

function TMySQLIniManipulator.get_loose_keyring_file_data: string;
begin
  result := ReadString(MYSQLD_SECTION_NAME, LOOSE_KEYRING_FILE_DATA_PROPERTY_NAME, '');
end;

procedure TMySQLIniManipulator.set_loose_keyring_file_data(value: string);
var
  _pathInLinuxStyle: string;
begin
  _pathInLinuxStyle := getPathInLinuxStyle(value);
  WriteString(MYSQLD_SECTION_NAME, LOOSE_KEYRING_FILE_DATA_PROPERTY_NAME, _pathInLinuxStyle);
end;

function TMySQLIniManipulator.get_secure_file_priv: string;
begin
  result := ReadString(MYSQLD_SECTION_NAME, SECURE_FILE_PRIV_PROPERTY_NAME, '');
end;

procedure TMySQLIniManipulator.set_secure_file_priv(value: string);
var
  _pathInLinuxStyle: string;
begin
  _pathInLinuxStyle := getPathInLinuxStyle(value);
  WriteString(MYSQLD_SECTION_NAME, SECURE_FILE_PRIV_PROPERTY_NAME, _pathInLinuxStyle);
end;

function TMySQLIniManipulator.getDatadir: string;
begin
  result := ReadString(MYSQLD_SECTION_NAME, DATADIR_PROPERTY_NAME, '');
end;

procedure TMySQLIniManipulator.setDatadir(value: string);
var
  _pathInLinuxStyle: string;
begin
  _pathInLinuxStyle := getPathInLinuxStyle(value);
  WriteString(MYSQLD_SECTION_NAME, DATADIR_PROPERTY_NAME, _pathInLinuxStyle);
end;

function TMySQLIniManipulator.get_innodb_buffer_pool_size: string;
begin
  result := ReadString(MYSQLD_SECTION_NAME, INNODB_BUFFER_POOL_SIZE_PROPERTY_NAME, '');
end;

procedure TMySQLIniManipulator.set_innodb_buffer_pool_size(value: string);
begin
  WriteString(MYSQLD_SECTION_NAME, INNODB_BUFFER_POOL_SIZE_PROPERTY_NAME, value);
end;

function TMySQLIniManipulator.get_innodb_log_file_size: string;
begin
  result := ReadString(MYSQLD_SECTION_NAME, INNODB_LOG_FILE_SIZE_PROPERTY_NAME, '');
end;

procedure TMySQLIniManipulator.set_innodb_log_file_size(value: string);
begin
  WriteString(MYSQLD_SECTION_NAME, INNODB_LOG_FILE_SIZE_PROPERTY_NAME, value);
end;

procedure TMySQLIniManipulator.setOptimizedInnodbSettings(raiseExceptionIfMemoryIsInsufficient: boolean = false);
const
  DEFAULT_INNODB_BUFFER_POOL_SIZE_PROPERTY = '8M';
  DEFAULT_INNODB_LOG_FILE_SIZE_PROPERTY = '48M';

  ERR_MSG_INSUFFICENT_MEMORY = 'Insufficient memory.';
var
  _totalFreeMemory: integer;
begin
  TMemoryRAM.initialize;
  _totalFreeMemory := TMemoryRAM.getTotalFreeMemoryAsInteger;
  case _totalFreeMemory of
    301 .. MaxInt:
      begin
        innodb_buffer_pool_size := '200M';
        innodb_log_file_size := '100M';
      end;
    201 .. 300:
      begin
        innodb_buffer_pool_size := '100M';
        innodb_log_file_size := '50M';
      end;
    101 .. 200:
      begin
        innodb_buffer_pool_size := '50M';
        innodb_log_file_size := '50M';
      end;
    0 .. 39:
      begin
        if raiseExceptionIfMemoryIsInsufficient then
        begin
          raise Exception.Create(ERR_MSG_INSUFFICENT_MEMORY);
        end;
      end;
  else
    begin
      innodb_buffer_pool_size := DEFAULT_INNODB_BUFFER_POOL_SIZE_PROPERTY;
      innodb_log_file_size := DEFAULT_INNODB_LOG_FILE_SIZE_PROPERTY;
    end;
  end;
end;

procedure TMySQLIniManipulator.setRequiredPathsInIni(path_datadir: string; path_secure_file_priv: string);
var
  _path_secure_file_priv: string;
begin
  _path_secure_file_priv := getValidFullPath(path_secure_file_priv);
  _path_secure_file_priv := getDoubleQuotedString(_path_secure_file_priv);
  secure_file_priv := _path_secure_file_priv;

  setRequiredPathsInIni(path_datadir);
end;

procedure TMySQLIniManipulator.setRequiredPathsInIni(path_datadir: string);
var
  _path_datadir: string;
begin
  _path_datadir := getValidFullPath(path_datadir);
  _path_datadir := getDoubleQuotedString(_path_datadir);
  datadir := _path_datadir;
end;

end.
