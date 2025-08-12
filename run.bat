@echo off
setlocal enabledelayedexpansion
REM MapReduce框架运行脚本
REM 运行WordCount应用

echo ===== MapReduce Framework Runner =====
echo.

REM 设置变量
set BUILD_DIR=build\classes
set MAIN_CLASS=app.wordcount.WordCountDriver
set INPUT_DIR=%1
set OUTPUT_DIR=%2

REM 检查编译结果
if not exist "%BUILD_DIR%" (
    echo Error: Build directory not found: %BUILD_DIR%
    echo Please run compile.bat first to compile the project.
    echo.
    goto :usage
)

if not exist "%BUILD_DIR%\app\wordcount\WordCountDriver.class" (
    echo Error: WordCountDriver.class not found in %BUILD_DIR%
    echo Please run compile.bat first to compile the project.
    echo.
    goto :usage
)

REM 检查参数
if "%INPUT_DIR%"=="" (
    echo Error: Input directory not specified.
    echo.
    goto :usage
)

if "%OUTPUT_DIR%"=="" (
    echo Error: Output directory not specified.
    echo.
    goto :usage
)

REM 检查输入目录
if not exist "%INPUT_DIR%" (
    echo Error: Input directory does not exist: %INPUT_DIR%
    echo.
    echo Creating sample input directory and test data...
    mkdir "%INPUT_DIR%"

    REM 生成测试数据
    echo Generating test data...
    java -cp "%BUILD_DIR%" test.TestDataGenerator "%INPUT_DIR%" 200

    if %ERRORLEVEL% NEQ 0 (
        echo Failed to generate test data.
        goto :end
    )

    echo Test data generated successfully in: %INPUT_DIR%
    echo.
)

REM 清理temp目录
if exist "temp" (
    echo Cleaning temp directory: temp
    rmdir /S /Q "temp" >nul 2>&1
)
mkdir "temp" >nul 2>&1
echo Temp directory prepared.
echo.

REM 清理输出目录
if exist "%OUTPUT_DIR%" (
    echo Cleaning output directory: %OUTPUT_DIR%
    rmdir /S /Q "%OUTPUT_DIR%" >nul 2>&1
)
mkdir "%OUTPUT_DIR%" >nul 2>&1

REM 显示运行信息
echo Running MapReduce WordCount application...
echo Input directory: %INPUT_DIR%
echo Output directory: %OUTPUT_DIR%
echo Main class: %MAIN_CLASS%
echo.
echo Starting execution...
echo ========================================
echo.

REM 记录开始时间
set START_TIME=%TIME%

REM 运行应用
java -cp "%BUILD_DIR%" %MAIN_CLASS% "%INPUT_DIR%" "%OUTPUT_DIR%" %3 %4 %5 %6 %7 %8 %9

REM 检查运行结果
if !ERRORLEVEL! EQU 0 (
    echo.
    echo =============================================
    echo ===== Execution Completed Successfully! =====

    REM 记录结束时间
    set END_TIME=!TIME!
    echo Start time: !START_TIME!
    echo End time: !END_TIME!
    echo.

    REM 显示输出文件
    echo Output files:
    if exist "!OUTPUT_DIR!\*.txt" (
        for %%f in ("!OUTPUT_DIR!\*.txt") do (
            echo   %%~nxf ^(%%~zf bytes^)
        )
    ) else (
        echo   No output files found.
    )

    echo.
    echo You can view the results in: !OUTPUT_DIR!

) else (
    echo.
    echo =============================
    echo ===== Execution Failed! =====
    echo Please check the error messages above.
    echo.
)

goto :end

:usage
echo Usage: run.bat ^<input_directory^> ^<output_directory^> [options]
echo.
echo Parameters:
echo   input_directory    Directory containing input text files (.txt)
echo   output_directory   Directory where output files will be written
echo.
echo Options:
echo   -mapTasks=^<num^>      Number of map tasks (default: 4)
echo   -reduceTasks=^<num^>   Number of reduce tasks (default: 2)
echo   -bufferSize=^<kb^>     Buffer size in MB (default: 1)
echo   -maxThreads=^<num^>    Maximum number of threads (default: 8)
echo   -noMonitoring        Disable performance monitoring
echo.
echo Examples:
echo   run.bat input output
echo   run.bat input output -mapTasks=6 -reduceTasks=3
echo   run.bat input output -bufferSize=200 -maxThreads=12
echo.
echo Note: If input directory doesn't exist, test data will be generated automatically.
echo.

:end
echo.
::pause