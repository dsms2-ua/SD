@echo off
setlocal enabledelayedexpansion

echo Programa de iniciar clientes

set /p Bootstrap_IP="Introduce la IP del Bootstrap: "
set /p Bootstrap_Puerto="Introduce el puerto del Bootstrap: "

set /p numClients="Introduce el número de clientes a crear: "

set letters=abcdefghijklmnopqrstuvwxyz


for /L %%i in (1,1,%numClients%-1) do (
    set "letter=!letters:~%%i,1!"
    start cmd /k "python EC_Customer.py  %Bootstrap_IP% %Bootstrap_Puerto% !letter!"
)