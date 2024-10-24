@echo off
setlocal enabledelayedexpansion
echo Programa de iniciar sensores

set /p Central_IP="Introduce la IP de la Central: "

set /p numSensores="Introduce el número de sensores por taxi: "
set /p numTaxis="Introduce el número de taxis existentes: "

echo Iniciando sensores...

for /L %%t in (1,1,%numTaxis%) do (
    echo Iniciando sensores para el taxi %%t
    set /A port=%%t+4999
    for /L %%s in (1,1,%numSensores%) do (
        echo Iniciando sensor %%s para el taxi %%t en el puerto !port!
        start cmd /k "python EC_S.py %Central_IP% !port!"
    )
)

echo Sensores iniciados.
pause