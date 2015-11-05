var AWS = require('aws-sdk');
var fs = require('fs');
var Promise = require('bluebird');

AWS.config.update({region: 'us-east-1'});
var config = JSON.parse(fs.readFileSync('config.json'));
var KEY_DELIM = '|';
var docClient = new AWS.DynamoDB.DocumentClient();
Promise.promisifyAll(docClient);

var DEFAULT_PERIOD = {
  name: 'hourly',
  modulus: 60*60*1000 // ms
};

exports.handler = function(event, context) {
  var timestamp = new Date().getTime();
  var bucket = timestamp - (timestamp % DEFAULT_PERIOD.modulus);
  Promise.all(config.breakdowns.map(function(breakdown) {
    var key = breakdown.dimensions.map(function(dimension) {
      return event[dimension];
    }).join(KEY_DELIM);

    return docClient.updateAsync({
      TableName: breakdown.name + '_' + DEFAULT_PERIOD.name, // TODO(ted): Make this configurable
      Key: {
        key: key,
        timestamp: bucket
      },
      UpdateExpression: 'SET #event_name = if_not_exists(#event_name, :zero) + :one',
      ExpressionAttributeNames: {
        '#event_name': event.event_name + '_count',
      },
      ExpressionAttributeValues: {
        ':zero': 0,
        ':one': 1
      }
    })
  })).then(context.succeed.bind(context), function(error) {
    console.log(error.message);
    context.fail(error.message);
  });
}
