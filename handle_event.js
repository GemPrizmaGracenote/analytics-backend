var AWS = require('aws-sdk');
var fs = require('fs');
var Promise = require('bluebird');
var _ = require('lodash');
var common = require('./common');

AWS.config.update({region: 'us-east-1'});
var config = JSON.parse(fs.readFileSync('config.json'));
var docClient = new AWS.DynamoDB.DocumentClient();
Promise.promisifyAll(docClient);

var KEY_DELIM = common.KEY_DELIM;
var DEFAULT_PERIOD = common.DEFAULT_PERIOD;

function colIncrementParams(breakdown, timestamp, rowKey, colKey, incrAmt) {
  return {
    TableName: breakdown.name + '_' + DEFAULT_PERIOD.name, // TODO(ted): Make this configurable
    Key: {
      key: rowKey,
      timestamp: timestamp
    },
    UpdateExpression: 'SET #col_key = if_not_exists(#col_key, :zero) + :amt',
    ExpressionAttributeNames: {'#col_key': colKey},
    ExpressionAttributeValues: {
      ':zero': 0,
      ':amt': incrAmt
    }
  }
}

var NULL_FILTER = {
  name: 'all',
  properties: []
}

exports.handler = function(event, context) {
  var timestamp = new Date().getTime();
  var bucket = timestamp - (timestamp % DEFAULT_PERIOD.modulus);
  var writes = [];
  config.breakdowns.forEach(function(breakdown) {
    var dimensions = breakdown.dimensions.concat(['event_name']);
    _.get(breakdown, 'filters', [NULL_FILTER]).forEach(function(filter) {
      var rowKey = common.buildRowKey(event, dimensions, filter.name, 'count');
      var colKey = common.buildColKey(event, filter.properties);
      if (!(rowKey && colKey)) {
        return;
      }
      writes.push(colIncrementParams(breakdown, bucket, rowKey, colKey, 1));
      config.extra_aggregations.forEach(function(agg) {
        var rowKey = common.buildRowKey(event, dimensions, filter.name, agg.name);
        var value = _.get(event, agg.property);
        if (rowKey && value) {
          var params = colIncrementParams(breakdown, bucket, rowKey, colKey, value);
          writes.push(params);
        }
      });
    });
  });
  console.log('Writing %d rows', writes.length);
  Promise.all(writes.map(function(write) {
    return docClient.updateAsync(write);
  })).then(context.succeed.bind(context), function(error) {
    console.error(error.message);
    context.fail(error.message);
  }).catch(context.fail);
}
