exports.KEY_DELIM = '|';

exports.DEFAULT_PERIOD = {
  name: 'hourly',
  modulus: 60*60*1000 // ms
};

exports.DEFAULT_METRIC_NAME = 'count';

function buildKey(event, dimensions) {
  var keyParts = dimensions.map(function(dimension) {
    return _.get(event, dimension);
  });
  if (!_.all(keyParts)) {
    return false;
  }
  return keyParts.join(KEY_DELIM);
}

exports.buildRowKey = function(event, dimensions, filterName, suffix) {
  var key = buildKey(event, dimensions);
  if (!key) {
    return false;
  }
  return [key, filterName, suffix].join('_');
}

exports.buildColKey = function(event, dimensions) {
  if (!dimensions.length) {
    return 'all';
  }
  return buildKey(event, dimensions);
}

exports.NULL_FILTER = {
    name: 'all',
    properties: []
};
