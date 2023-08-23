#define MyAppIco "favicon.ico"
#define MyAppBeforeInstallTxt "info_before_install.txt"
#define MyAppDir "C:\Program Files"
#define AppName "taos-odbc"

[Setup]
AppName={#AppName}
AppVersion={#MyAppVersion}
DefaultDirName={#MyAppDir}
InfoBeforeFile={#MyAppBeforeInstallTxt}
SetupIconFile={#MyAppIco}
Compression=lzma
SolidCompression=yes
DisableDirPage=yes
Uninstallable=yes
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "{#MyAppSourceDir}\*"; DestDir: "{app}\"; Flags: recursesubdirs

[run]
Filename: "{cmd}"; Parameters: "/c odbcconf /F ""C:\Program Files\taos_odbc\win_odbcinst.in"""; WorkingDir: "{app}"; Flags: runhidden; StatusMsg: "Configuring ODBC"



[UninstallDelete]
Type: files; Name: "{app}\taos_odbc\*.*"
                                                       
[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
