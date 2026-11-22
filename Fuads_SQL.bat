@echo off
title Fuad's SQL console
cd /d "%~dp0"
where py >nul 2>nul && (py -3 fuads_sql.py %*) || (python fuads_sql.py %*)
pause
