Set WshShell = CreateObject("WScript.Shell")
Set FileSystem = CreateObject("Scripting.FileSystemObject")

scriptDir = FileSystem.GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = scriptDir

WshShell.Run "cmd.exe /d /c call run_edge_management.bat", 0, False

Set FileSystem = Nothing
Set WshShell = Nothing
