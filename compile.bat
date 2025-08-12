@echo off
REM MapReduce框架编译脚本
REM 编译所有Java源文件

echo ===== MapReduce Framework Compilation =====
echo.

REM 设置变量
set SRC_DIR=src\main\java
set BUILD_DIR=build\classes
set LIB_DIR=lib

REM 创建构建目录
if not exist "%BUILD_DIR%" (
    echo Creating build directory: %BUILD_DIR%
    mkdir "%BUILD_DIR%"
)

REM 清理之前的编译结果
echo Cleaning previous build...
if exist "%BUILD_DIR%\*" (
    del /Q /S "%BUILD_DIR%\*" >nul 2>&1
)

REM 编译所有Java文件
echo Compiling Java source files...
echo.

REM 查找所有Java文件
dir /S /B "%SRC_DIR%\*.java" > temp_files.txt

REM 编译
javac -encoding UTF-8 -d "%BUILD_DIR%" -cp "%BUILD_DIR%" @temp_files.txt

REM 检查编译结果
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ===== Compilation Successful! =====
    echo Compiled classes are in: %BUILD_DIR%
    echo.
    
    REM 复制资源文件
    if exist "src\main\resources" (
        echo Copying resource files...
        xcopy "src\main\resources\*" "%BUILD_DIR%" /E /I /Y >nul 2>&1
        echo Resource files copied.
    )
    
    REM 显示编译统计
    echo.
    echo Compilation Statistics:
    for /f %%i in ('dir /S /B "%BUILD_DIR%\*.class" ^| find /c ".class"') do echo   Total .class files: %%i
    
    echo.
    echo You can now run the application using run.bat
    echo Example: run.bat input output
    
) else (
    echo.
    echo ===== Compilation Failed! =====
    echo Please check the error messages above and fix the issues.
    echo.
)

REM 清理临时文件
if exist temp_files.txt del temp_files.txt

echo.
::pause