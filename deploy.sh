set -e
FUNCTION_NAMES="handleEvent aggregateEvents"
PROJECT_DIR=.
BUILT_ZIP=/tmp/handleEvent.zip
npm install
zip -r /tmp/handleEvent.zip `git ls-files` node_modules
for fn in $FUNCTION_NAMES; do
  aws lambda update-function-code --function-name $fn --zip-file fileb://$BUILT_ZIP
done
