@pwd
@set /p ut="Update type (patch|minor|major): "
@call npm version %ut%
@call npm publish
@pause