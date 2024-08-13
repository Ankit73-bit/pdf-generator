@echo off
setlocal
set GS_OUTPUT_DIR=..\COMPRESS
mkdir %GS_OUTPUT_DIR%

for %%i in (*.pdf) do (
    qpdf --stream-data=uncompress "%%i" "%GS_OUTPUT_DIR%\temp.pdf"
    ps2pdf "%GS_OUTPUT_DIR%\temp.pdf" "%GS_OUTPUT_DIR%\%%i"
    del "%GS_OUTPUT_DIR%\temp.pdf"
)
