call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
REM call C:\Users\89509001\Anaconda3\Scripts\activate.bat C:\Users\89509001\Anaconda3
call conda activate scrapy_01
scrapy crawl multas_de_transito -a filename=input.txt
pause
