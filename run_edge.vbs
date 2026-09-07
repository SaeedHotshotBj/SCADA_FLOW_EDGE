Set WshShell = CreateObject("WScript.Shell")

scriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
batFile = scriptDir & "\run_edge.bat"

WshShell.Run Chr(34) & batFile & Chr(34), 0, False

Set WshShell = Nothing
