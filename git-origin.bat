@pwd
@for %%I in (.) do @set repo=%%~nxI
@git init
@git add -A
@git commit -m "initial commit"
@git remote add origin https://github.com/fernandobhz/%repo%.git
@git push -u origin master
@pause

