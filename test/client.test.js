var assert = require('assert');
var client = require('../client');
var common = require('../common');
var moment = require('moment');

describe('parseTimeframe', function() {
  it('should parse a relative range with this', function() {
    var now = moment(1447381538660);
    var result = client.parseTimeframe(now, 'this_1_day', 'Americas/Pacific');
    assert.equal(result[0], new Date(2015, 10, 12).getTime());
    assert.equal(result[1], null);
  });
});

var config = {
  "breakdowns": [
    {"name": "video_performance", "dimensions": ["video_id", "unit_domain"]},
    {
      "name": "publisher_metrics",
      "dimensions": [
        "unit_environment",
        "unit_pbid",
        "unit_domain"
      ],
      "filters": [
        {"name": "geo", "properties": ["ip_geo_info.country"]},
        {"name": "device_layout", "properties": ["user_agent_device", "unit_layout"]}
      ]
    }
  ],
  "extra_aggregations": [
    {"property": "unit_video_list.length", "name": "total_impressions"}
  ]
};

describe('findSource', function() {

  it('should find a source where all eq filters match dimensions', function() {
    var queryFilters = [
      {property_name: 'video_id', operator: 'eq', property_value: 'foo'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown.name, 'video_performance');
    assert.deepEqual(source.filter, common.NULL_FILTER);
  });

  it('should find a source where all dimensions match and one filter matches', function() {
    var queryFilters = [
      {property_name: 'unit_environment', operator: 'eq', property_value: 'prod'},
      {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
      {property_name: 'ip_geo_info.country', operator: 'neq', property_value: 'US'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown.name, 'publisher_metrics');
  });

  it('should find a source where all dimensions match and one group by matches', function() {
    var queryFilters = [
      {property_name: 'unit_environment', operator: 'eq', property_value: 'prod'},
      {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'}
    ];
    var source = client.findSource(config, queryFilters, ['ip_geo_info.country']);
    assert.equal(source.breakdown.name, 'publisher_metrics');
  });

  it('should not find a source when not all dimensions match', function() {
    var queryFilters = [
      {property_name: 'unit_environment', operator: 'eq', property_value: 'prod'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
      {property_name: 'ip_geo_info.country', operator: 'neq', property_value: 'US'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown, null);
    assert.equal(source.filter, null);
  });

  it('should not find a source when a dimensional query filter operator isn\'t eq', function() {
    var queryFilters = [
      {property_name: 'unit_environment', operator: 'eq', property_value: 'prod'},
      {property_name: 'unit_pbid', operator: 'ne', property_value: 'pb-1234'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown, null);
    assert.equal(source.filter, null);
  });

  it('should not find a source when a query filter doesn\'t match any source filters', function() {
    var queryFilters = [
      {property_name: 'unit_environment', operator: 'eq', property_value: 'prod'},
      {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
      {property_name: 'non_existant', operator: 'ne', property_value: 'bar'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown, null);
    assert.equal(source.filter, null);
  });

  it('should match the null filter for a source with no filters', function() {
    var queryFilters = [
      {property_name: 'video_id', operator: 'eq', property_value: 'foo'},
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
    ];
    var source = client.findSource(config, queryFilters, []);
    assert.equal(source.breakdown.name, 'video_performance');
    assert.equal(source.filter.name, 'all');

  });
});

describe('filter', function() {
  it('should filter a row with an eq test', function() {
    var sourceFilters = ['unit_domain', 'video_id'];
    var queryFilters = [
      {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
      {property_name: 'video_id', operator: 'eq', property_value: 'bar'}
    ];
    var item = {'foo.com|bar': 5, 'baz.com|qux': 10};
    var result = client.filter(item, queryFilters, sourceFilters);
    assert.deepEqual(result, {'foo.com|bar': 5});
  });

  it('should filter a row with a ne test', function() {
    var sourceFilters = ['unit_domain', 'video_id'];
    var queryFilters = [
      {property_name: 'unit_domain', operator: 'ne', property_value: 'foo.com'},
      {property_name: 'video_id', operator: 'ne', property_value: 'bar'}
    ];
    var item = {'foo.com|bar': 5, 'baz.com|qux': 10};
    var result = client.filter(item, queryFilters, sourceFilters);
    assert.deepEqual(result, {'baz.com|qux': 10});
  });
});

describe('aggregate', function() {
  it('should aggregate an item with no group by', function() {
    var item = {'foo.com|bar': 5, 'baz.com|qux': 10};
    var result = client.aggregate(item);
    assert.equal(result, 15);
  });

  it('should aggregate an item with a group by', function() {
    var item = {'foo.com|bar': 5, 'baz.com|qux': 10, 'baz.com|abcd': 3};
    var result = client.aggregate(item, ['unit_domain'], ['unit_domain', 'video_id']);
    assert.deepEqual(result, [
      {'unit_domain': 'foo.com', 'result': 5},
      {'unit_domain': 'baz.com', 'result': 13}
    ]);

  });
});

var TEST_ROWS_1 = [{
  key: 'PRODUCTION|PB-1234|foo.com|UnitFirstLoad_geo_count',
  timestamp: 1447671600000,
  'US': 5,
  'DE': 7
}, {
  key: 'PRODUCTION|PB-1234|foo.com|UnitFirstLoad_geo_count',
  timestamp: 1447675200000,
  'US': 9,
  'DE': 7
}];

var TEST_ROWS_2 = [{
  key: 'PRODUCTION|PB-234|foo.com|UnitFirstLoad_geo_count',
  timestamp: 1447678800000,
  'US': 2
}];

describe('IntervalQueryBuilder', function() {
  it('should return results for hourly', function() {
    var iqb = client.makeQueryBuilder(config, {
      filters: [
        {property_name: 'unit_environment', operator: 'eq', property_value: 'PRODUCTION'},
        {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
        {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
        {property_name: 'ip_geo_info.country', operator: 'eq', property_value: 'US'},
      ],
      interval: 'hourly',
      timeframe: 'this_24_hours',
      eventCollection: 'UnitFirstLoad',
      timezone: 'America/Los_Angeles'
    });

    iqb.addRows(TEST_ROWS_1);
    iqb.addRows(TEST_ROWS_2);
    assert.deepEqual(iqb.getResult(), {
      result: [
        {timeframe: {start: '2015-11-16T11:00:00.000Z', end: '2015-11-16T12:00:00.000Z'}, value: 5},
        {timeframe: {start: '2015-11-16T12:00:00.000Z', end: '2015-11-16T13:00:00.000Z'}, value: 9},
        {timeframe: {start: '2015-11-16T13:00:00.000Z', end: '2015-11-16T14:00:00.000Z'}, value: 2}
      ]
    });
  });
  it('should return results for daily', function() {
    var iqb = client.makeQueryBuilder(config, {
      filters: [
        {property_name: 'unit_environment', operator: 'eq', property_value: 'PRODUCTION'},
        {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
        {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
        {property_name: 'ip_geo_info.country', operator: 'eq', property_value: 'US'},
      ],
      interval: 'daily',
      timeframe: 'this_24_hours',
      eventCollection: 'UnitFirstLoad',
      timezone: 'America/Los_Angeles'
    });

    iqb.addRows(TEST_ROWS_1);
    iqb.addRows(TEST_ROWS_2);
    assert.deepEqual(iqb.getResult(), {
      result: [
        {timeframe: {start: '2015-11-16T00:00:00.000Z', end: '2015-11-17T00:00:00.000Z'}, value: 16}
      ]
    });
  })
});

describe('SingleValueQueryBuilder', function() {
  it('should return results', function() {
    var iqb = client.makeQueryBuilder(config, {
      filters: [
        {property_name: 'unit_environment', operator: 'eq', property_value: 'PRODUCTION'},
        {property_name: 'unit_pbid', operator: 'eq', property_value: 'pb-1234'},
        {property_name: 'unit_domain', operator: 'eq', property_value: 'foo.com'},
        {property_name: 'ip_geo_info.country', operator: 'eq', property_value: 'US'},
      ],
      timeframe: 'this_24_hours',
      eventCollection: 'UnitFirstLoad',
      timezone: 'America/Los_Angeles'
    });

    iqb.addRows([{
      key: 'PRODUCTION|PB-1234|foo.com|UnitFirstLoad_geo_count',
      timestamp: 1447671600000,
      'US': 5,
      'DE': 7
    }, {
      key: 'PRODUCTION|PB-1234|foo.com|UnitFirstLoad_geo_count',
      timestamp: 1447675200000,
      'US': 9,
      'DE': 7
    }]);
    iqb.addRows([{
      key: 'PRODUCTION|PB-234|foo.com|UnitFirstLoad_geo_count',
      timestamp: 1447678800000,
      'US': 2
    }]);
    assert.deepEqual(iqb.getResult(), {
      result: 16
    });
    assert.equal(iqb.tableName, 'publisher_metrics_hourly');
  });
});
