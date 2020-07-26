@echo off
TITLE folderclone
for /f "tokens=*" %%a in ('chcp') do set result=%%a
py -3.7 -m clonerbot.py "%result%"
