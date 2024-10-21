@echo off
echo Programa de iniciar taxi

set /p Central_IP="Introduce la IP de la Central: "
set /p Central_Puerto="Introduce el puerto de la Central: "

set /p Bootstrap_IP="Introduce la IP del Bootstrap: "
set /p Bootstrap_Puerto="Introduce el puerto del Bootstrap: "

set /p numTaxis="Introduce el número de taxis a crear: "

for /L %%i in (1,1,%numTaxis%) do (
    start cmd /k "python EC_DE.py %Central_IP% %Central_Puerto% %Bootstrap_IP% %Bootstrap_Puerto% %%i"
)