var AWS = require('aws-sdk');
var fs = require('fs');
var Promise = require('bluebird');
var _ = require('lodash');
var moment = require('moment-timezone');
var common = require('./common');

AWS.config.update({region: 'us-east-1'});
var config = JSON.parse(fs.readFileSync('config.json'));
var docClient = new AWS.DynamoDB.DocumentClient();
Promise.promisifyAll(docClient);

var KEY_DELIM = common.KEY_DELIM;
var DEFAULT_PERIOD = common.DEFAULT_PERIOD;

var INTERAVAL_SIZES = {
  'hour': 60*60*1000,
  'day': 24*60*60*1000,
  'week': 7*24*60*60*1000,
};

function depluralize(str) {
  return str.replace(/s$/, '');
}

/**
 * Parse a Keen-style timeframe (e.g., 'this_10_hours' into a range).
 *
 * See https://keen.io/docs/api/#timeframe for full specification.
 *
 * Returns a two element array of [start_offset, end_offset].
 */
function parseTimeframe(now, range, timezone) {
  if (!timezone) {
    timezone = 'UTC';
  }
  var parts = range.split('_');
  var this_or_prev = parts.shift();
  var interval, start, end;

  if (parts.length == 1) {
    num = 1;
    interval = parts[0];
  } else if (parts.length == 2) {
    num = parseInt(parts[0], 10);
    interval = depluralize(parts[1]);
  } else {
    throw new Error('Invalid timeframe: ' + range);
  }

  if (this_or_prev === 'this') {
    end = null;
    start = moment(now).tz(timezone).startOf(interval).subtract(num-1, interval);
  } else if (this_or_prev === 'prev') {
    var end = moment(now).tz(timezone).startOf(interval);
    var start = moment(end).tz(timezone).subtract(num, interval);
  }
  return [start.valueOf(), end ? end.valueOf() : null];
}

/**
 * Finds the source breakdown and filter for the query filters provided.
 *
 * Takes an array of filters, where each filter is an object of the form:
 * {property_name: "name", operator: "op", property_value: "value"}.
 *
 * For more information, see the design doc here:
 * https://docs.google.com/a/fem-inc.com/document/d/1On6HLcMTq1M5JPA31l_BT_Qn9WjLOv8TiEP2d3Viq94/edit?usp=sharing
 *
 * Returns a breakdown and filter.
 */
function findSource(config, queryFilters, groupBys) {
  var keyOrFilterProperties = queryFilters.filter(function(filter) {
    return filter.operator == 'eq';
  }).map(function(filter) {
    return filter.property_name;
  });
  var filterOnlyProperties = queryFilters.filter(function(filter) {
    return filter.operator != 'eq';
  }).map(function(filter) {
    return filter.property_name; 
  }).concat(groupBys);
  function isSubset(a, b) {
    return _.isEmpty(_.difference(a, b));
  }

  for (var i = 0; i < config.breakdowns.length; i++) {
    var breakdown = config.breakdowns[i];
    var filtersToCheck = [common.NULL_FILTER].concat((breakdown.filters || []));
    for (var j = 0; j < filtersToCheck.length; j++) {
      var filter = filtersToCheck[j];
      if (isSubset(breakdown.dimensions, keyOrFilterProperties) &&
         isSubset(_.difference(keyOrFilterProperties, breakdown.dimensions), filter.properties) &&
           isSubset(filterOnlyProperties, filter.properties)) {
        return {breakdown: breakdown, filter: filter};
      }
    }
  }
  return {breakdown: null, filter: null};
}

function buildRowKeyFromFilters(source, lookupFilters, eventName) {
  var filterMap = {event_name: eventName};
  lookupFilters.forEach(function(filter) {
    filterMap[filter.property_name] = filter.property_value;
  });
  var dimensions = source.breakdown.dimensions.concat(['event_name']);
  return common.buildRowKey(filterMap, dimensions, source.filter.name, common.DEFAULT_METRIC_NAME);
}

function doQuery(tableName, key, range, opt_lastEvaluatedKey) {
  var expr;
  var attributeValues;
  if (range[1]) {
    expr = '#timestamp BETWEEN :start and :end';
    attributeValues = {
      ':start': range[0],
      ':end': range[1]
    };
  } else {
    expr = '#timestamp >= :start';
    attributeValues = {
      ':start': range[0]
    };
  }
  expr = '#key = :key AND ' + expr;
  attributeValues[':key'] = key;

  var params = {
    TableName: tableName,
    KeyConditionExpression: expr,
    ExpressionAttributeNames: {
      '#key': 'key',
      '#timestamp': 'timestamp'
    },
    ExpressionAttributeValues: attributeValues,
    ExclusiveStartKey: opt_lastEvaluatedKey
  };
  return docClient.queryAsync(params);
}

function decodeKey(key, names) {
  var out = {};
  var parts = key.split(common.KEY_DELIM);
  _.zip(names, parts).forEach(function(elem) {
    out[elem[0]] = elem[1];
  });
  return out;
}

function filter(item, queryFilters, sourceFilterProperties) {
  var queryFilterMap = {};
  queryFilters.forEach(function(filter) {
    queryFilterMap[filter.property_name] = filter;
  });
  var result = {};
  for (var colKey in item) {
    var decoded = decodeKey(colKey, sourceFilterProperties);
    var matches = true;
    queryFilters.forEach(function(filter) {
      var colValue = decoded[filter.property_name]
      switch(filter.operator) {
        case 'eq':
          if (colValue !== filter.property_value) {
            matches = false;
          }
          break;
        case 'ne':
          if (colValue === filter.property_value) {
            matches = false;
          }
          break;
      }
    });
    if (matches) {
      result[colKey] = item[colKey];
    }
  }
  return result;
}

function aggregate(item, groupBy, sourceFilterProperties) {
  if (!_.isArray(groupBy) || _.isEmpty(groupBy)) {
    return _.sum(item);
  } else {
    var grouped = {};
    _.forEach(item, function(val, colKey) {
      var decoded = decodeKey(colKey, sourceFilterProperties);
      var groupByKey = common.buildColKey(decoded, groupBy);
      grouped[groupByKey] = (grouped[groupByKey] || 0) + val;
    });
    return _.map(grouped, function(sum, colKey) {
      var out = decodeKey(colKey, groupBy);
      out.result = sum;
      return out;
    });
  }
}

exports.handler = function(query, context) {
  var range = parseTimeframe(moment(), query.timeframe);
  var groupBy = [];
  if (_.isArray(query.groupBy)) {
    groupBy = query.groupBy;
  } else if (_.isString(query.groupBy)) {
    groupBy = [query.groupBy];
  }
  var source = findSource(config, query.filters, groupBy);
  var queryFilters = query.filters || [];
  var eventName = query.eventCollection;
  if (!source.breakdown) {
    return context.fail('A source for this request could not be found.');
  }
  var tableName = source.breakdown.name + '_' + common.DEFAULT_PERIOD.name;
  var key = buildRowKeyFromFilters(source, queryFilters, eventName);
  // The query filters that were used to match the row shouldn't also be used
  // to match the column.  We discard them here.
  var unmatchedFilters = queryFilters.filter(function(filter) {
    return !_.contains(source.breakdown.dimensions, filter.property_name);
  });
  doQuery(tableName, key, range).then(function(data) {
    var row = data.Items.reduce(function(prev, current) {
      for (var key in current) {
        if (key !== 'timestamp' && current.hasOwnProperty(key) && _.isNumber(current[key])) {
          prev[key] = (_.isNumber(prev[key]) ? prev[key] : 0) + current[key]
        }
      }
      return prev;
    }, {});
    if (data.LastEvaluatedKey) {
      return doQuery(tableName, key, range, data.LastEvaluatedKey);
    } else {
      return row;
    }
  }).then(function(row) {
    var filtered = filter(row, unmatchedFilters, source.filter.properties);
    var result = aggregate(filtered, groupBy, source.filter.properties);
    var outTimeframe = {
      'start': moment(range[0]).toJSON()
    };
    if (range[1]) {
      outTimeframe['end'] = moment(range[1]).toJSON();
    }
    return {
      'timeframe': outTimeframe,
      'result': result
    };
  }).then(context.succeed, context.fail);
}

exports.findSource = findSource;
exports.parseTimeframe = parseTimeframe;
exports.filter = filter;
exports.aggregate = aggregate;
