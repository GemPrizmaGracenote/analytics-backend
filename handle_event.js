var AWS = require('aws-sdk');
var fs = require('fs');
var Promise = require('bluebird');
var _ = require('lodash');

AWS.config.update({region: 'us-east-1'});
var config = JSON.parse(fs.readFileSync('config.json'));
var KEY_DELIM = '|';
var docClient = new AWS.DynamoDB.DocumentClient();
Promise.promisifyAll(docClient);

var DEFAULT_PERIOD = {
  name: 'hourly',
  modulus: 60*60*1000 // ms
};

function incrementExpression(name, incr) {
  return name + ' = if_not_exists(' + name + ', :zero) + ' + incr;
}

function buildExtraAggregationParams(aggregations, event) {
  var expressionAttributeNames = {};
  var expressionAttributeValues = {};
  var updateExpression = [''];
  aggregations.forEach(function(agg, i) {
    var value = _.get(event, agg.property);
    if (!_.isNumber(value)) {
      return;
    }
    var sym = 'a' + i;
    var attrName = '#' + sym;
    var attrValue = ':' + sym;
    expressionAttributeNames[attrName] = event.event_name + '_' + agg.name;
    expressionAttributeValues[attrValue] = value;
    updateExpression.push(incrementExpression(attrName, attrValue));
  });
  return {
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues,
    UpdateExpression: updateExpression.join(', ')
  };
}

exports.handler = function(event, context) {
  var timestamp = new Date().getTime();
  var bucket = timestamp - (timestamp % DEFAULT_PERIOD.modulus);
  Promise.all(config.breakdowns.map(function(breakdown) {
    var keyParts = breakdown.dimensions.map(function(dimension) {
      return event[dimension];
    });
    if (!_.all(keyParts)) {
      return true;
    }
    var key = keyParts.join(KEY_DELIM);
    var aggregationParams = buildExtraAggregationParams(config.extra_aggregations, event);

    return docClient.updateAsync({
      TableName: breakdown.name + '_' + DEFAULT_PERIOD.name, // TODO(ted): Make this configurable
      Key: {
        key: key,
        timestamp: bucket
      },
      UpdateExpression: 'SET ' + incrementExpression('#event_count', ':one') +
          aggregationParams.UpdateExpression,
      ExpressionAttributeNames: _.assign({
        '#event_count': event.event_name + '_count',
      }, aggregationParams.ExpressionAttributeNames),
      ExpressionAttributeValues: _.assign({
        ':zero': 0,
        ':one': 1
      }, aggregationParams.ExpressionAttributeValues)
    })
  })).then(context.succeed.bind(context), function(error) {
    console.log(error.message);
    context.fail(error.message);
  });
}
