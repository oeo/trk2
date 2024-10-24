// Generated by CoffeeScript 2.7.0
(function() {
  // vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
  var Metrics, Redis, metrics, redis;

  Redis = require('ioredis');

  Metrics = require('./../module');

  redis = new Redis();

  metrics = new Metrics({
    redis: redis,
    key: 'examples'
  });

  (async function() {
    var addValue, e, impressions, r, topGeos, topOffers, unique;
    try {
      // return stats from the last ten days
      r = (await metrics.queryDays(10));
      // get unique visitor counts
      unique = r.find({
        type: 'bmp',
        key: 'ip'
      });
      console.log("Unique visitors:");
      console.log(unique);
      // addv
      addValue = r.find({
        type: 'addv',
        key: 'offer~event',
        addKey: 'amount'
      });
      console.log("Add value for key amount:");
      console.log(addValue);
      // get merged event counts for time duration of query
      impressions = r.find({
        type: 'add',
        key: 'event',
        merge: false
      });
      console.log("Impressions:");
      console.log(impressions);
      // get the top referring hostnames each day, ordered by frequency
      // argument is in alternative-shorthand syntax delimited by `/`
      // syntax: <type>/<keys>/[timestamp]
      topGeos = r.find('top/geo');
      console.log("Top geos:");
      console.log(topGeos);
      topOffers = r.find('top/offer');
      console.log("Top offers:");
      return console.log(topOffers);
    } catch (error) {
      e = error;
      return console.error("Error:", e);
    } finally {
      process.exit(0);
    }
  })();

}).call(this);
