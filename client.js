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

var INTERVAL_SIZES = {
  'hourly': 60*60*1000,
  'daily': 24*60*60*1000,
  'weekly': 7*24*60*60*1000,
};

INTERVAL_TO_NOUN = {
  'hourly': 'hour',
  'daily': 'day',
  'weekly': 'week',
  'monthly': 'month'
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

function BaseQueryBuilder(config, params) {
  this.params = params;
  if (_.isArray(params.groupBy)) {
    this.groupBy = params.groupBy;
  } else if (_.isString(params.groupBy)) {
    this.groupBy = [params.groupBy];
  } else {
    this.groupBy = [];
  }
  var eventName = params.eventCollection;
  this.source = findSource(config, params.filters, this.groupBy);

  if (!this.source.breakdown) {
    throw new Error('A source for this request could not be found.');
  }

  this.timezone = params.timezone || 'UTC';
  this.range = parseTimeframe(moment(), params.timeframe, this.timezone);
  this.tableName = this.source.breakdown.name + '_' + common.DEFAULT_PERIOD.name;
  this.key = buildRowKeyFromFilters(this.source, params.filters || [], eventName);
}

BaseQueryBuilder.prototype.filterAggregateSingleRow_ = function(row) {
  var source = this.source;
  // The query filters that were used to match the row shouldn't also be used
  // to match the column.  We discard them here.
  var unmatchedFilters = this.params.filters.filter(function(filter) {
    return !_.contains(source.breakdown.dimensions, filter.property_name);
  });
  var filtered = filter(row, unmatchedFilters, source.filter.properties);
  return aggregate(filtered, this.groupBy, source.filter.properties);
};

function rollUpRows(prevRow, newRows) {
  return newRows.reduce(function(prev, current) {
    for (var key in current) {
      if (!current.hasOwnProperty(key) || key === 'timestamp' || key === 'key') {
        continue;
      }
      if (_.isNumber(current[key])) {
        prev[key] = (_.isNumber(prev[key]) ? prev[key] : 0) + current[key]
      }
    }
    return prev;
  }, prevRow)
}

function SingleValueQueryBuilder(config, params) {
  BaseQueryBuilder.call(this, config, params);
  this.row = {};
}

SingleValueQueryBuilder.prototype = _.create(BaseQueryBuilder.prototype);

SingleValueQueryBuilder.prototype.addRows = function(rows) {
  this.row = rollUpRows(this.row, rows);
};

SingleValueQueryBuilder.prototype.getResult = function() {
  var value = this.filterAggregateSingleRow_(this.row);
  return {
    result: value
  };
}

function IntervalQueryBuilder(config, params) {
  BaseQueryBuilder.call(this, config, params);
  this.interval = params.interval;
  this.rows = {}
}

IntervalQueryBuilder.prototype = _.create(BaseQueryBuilder.prototype);

IntervalQueryBuilder.prototype.addRows = function(rows) {
  rows.forEach(function(row) {
    var timeframe = this.intervalToNoun();
    var bucket = row.timestamp - (row.timestamp % INTERVAL_SIZES[this.interval]);
    this.rows[bucket] = this.rows[bucket] || {};
    this.rows[bucket] = rollUpRows(this.rows[bucket], [row]);
  }, this);
};

IntervalQueryBuilder.prototype.intervalToNoun = function() {
  return INTERVAL_TO_NOUN[this.interval];
}

IntervalQueryBuilder.prototype.getResult = function() {
  var values = _.map(this.rows, function(row, timestamp) {
    var justValues = _.omit(row, ['key', 'timestamp']);
    return {
      timeframe: this.getTimeframeFromTimestamp_(parseInt(timestamp, 10)),
      value: this.filterAggregateSingleRow_(justValues)
    };
  }, this);
  var sorted = _.sortBy(values, 'timeframe.start');
  return {result: sorted};
}

IntervalQueryBuilder.prototype.getTimeframeFromTimestamp_ = function(timestamp) {
  var start = moment(timestamp);
  var end = start.clone().add(1, this.intervalToNoun());
  return {
    start: start.toJSON(),
    end: end.toJSON()
  };
};

function makeQueryBuilder(config, query) {
  if (query.interval) {
    return new IntervalQueryBuilder(config, query);
  } else {
    return new SingleValueQueryBuilder(config, query);
  }
}

exports.handler = function(query, context) {
  try {
    var queryBuilder = makeQueryBuilder(config, query)
  } catch(e) {
    return context.fail(e.message);
  }
  var tableName = queryBuilder.tableName;
  var key = queryBuilder.key;
  var range = queryBuilder.range;
  console.log('Query: %j\nusing source %s and filter %s', query,
              queryBuilder.source.breakdown.name, queryBuilder.source.filter.name);
  console.log('Timeframe: %j', range);
  doQuery(tableName, key, range).then(function(data) {
    console.log('Got response, %d rows', data.Items.length)
    queryBuilder.addRows(data.Items);
    if (data.LastEvaluatedKey) {
      return doQuery(tableName, key, range, data.LastEvaluatedKey);
    }
  }).then(function() {
    return queryBuilder.getResult();
  }).then(context.succeed, context.fail);
}

exports.findSource = findSource;
exports.parseTimeframe = parseTimeframe;
exports.filter = filter;
exports.makeQueryBuilder = makeQueryBuilder;
exports.aggregate = aggregate;
