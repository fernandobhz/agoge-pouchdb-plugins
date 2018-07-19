@pwd
@git tag
@set /p x=New tag:
@git tag %x% 
@git push --tags 
@pause
