Set WshShell = CreateObject("WScript.Shell")
Set FileSystem = CreateObject("Scripting.FileSystemObject")

scriptDir = FileSystem.GetParentFolderName(WScript.ScriptFullName)
batFile = FileSystem.BuildPath(scriptDir, "run_edge_management.bat")

WshShell.Run "cmd.exe /d /c " & Chr(34) & batFile & Chr(34), 0, False

Set FileSystem = Nothing
Set WshShell = Nothing
