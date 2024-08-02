@echo off
setlocal

set ENV_NAME=myenv

:: Prompt the user to enter the path to the Python 3.10 executable
:: example >> D:\Users\anyamanee\AppData\Local\Programs\Python\Python310\python.exe
echo Enter the path to your Python 3.10 executable:
set /p PYTHON_PATH="Path: "

:: Check if PYTHON_PATH is not empty
if "%PYTHON_PATH%"=="" (
    echo Python path was not provided. Exiting.
    goto end
)

echo Creating virtual environment named %ENV_NAME%...
"%PYTHON_PATH%" -m venv %ENV_NAME%

if errorlevel 1 (
    echo Failed to create virtual environment.
    goto end
)

echo Activating virtual environment...
call %ENV_NAME%\Scripts\activate.bat

if errorlevel 1 (
    echo Failed to activate the virtual environment.
    goto end
)

echo Upgrading pip...
"%PYTHON_PATH%" -m pip install --upgrade pip

echo Installing packages from requirements.txt...
"%PYTHON_PATH%" -m pip install -r requirements.txt

echo Setup is complete. Virtual environment '%ENV_NAME%' is ready to use.
echo To activate the virtual environment later, run: call %ENV_NAME%\Scripts\activate.bat

:end
endlocal
