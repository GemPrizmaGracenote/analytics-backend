set -e
FUNCTION_NAME=handleEvent
PROJECT_DIR=.
BUILT_ZIP=/tmp/handleEvent.zip
npm install
zip -r /tmp/handleEvent.zip `git ls-files` node_modules
aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://$BUILT_ZIP
