@pwd
@set /p s=Commit Message: 
@set /p ut="Update type (patch|minor|major): "
@git add -A 
@git commit -m "%s%"
@git push 
@call npm version %ut%
@call npm publish
@pause
