// Generated by CoffeeScript 2.7.0
(function() {
  // vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
  var Metrics, Redis, _, englishSecs, minimatch, timebase,
    indexOf = [].indexOf,
    splice = [].splice;

  _ = require('lodash');

  Redis = require('ioredis');

  minimatch = require('minimatch');

  englishSecs = require('./english-secs');

  timebase = require('./timebase');

  module.exports = Metrics = class Metrics {
    constructor(opt = {}) {
      var k, ref, ref1, ref2, ref3, ref4, v;
      this.redis = (ref = (ref1 = opt.redis) != null ? ref1 : opt.client) != null ? ref : new Redis();
      this.key = (ref2 = (ref3 = opt.key) != null ? ref3 : opt.prefix) != null ? ref2 : 'trk2';
      this.map = {
        bmp: [],
        add: [],
        addv: [],
        top: []
      };
      if (opt.map) {
        ref4 = opt.map;
        for (k in ref4) {
          v = ref4[k];
          this.map[k] = v;
        }
      }
      this.redis.defineCommand("getIdentityAndSetBit", {
        numberOfKeys: 2,
        lua: `local idMapKey = KEYS[1]
local bmpKey = KEYS[2]
local id = ARGV[1]

local identity = redis.call("ZSCORE", idMapKey, id)
if not identity then
  identity = redis.call("ZCARD", idMapKey)
  redis.call("ZADD", idMapKey, identity, id)
end

redis.call("SETBIT", bmpKey, identity, 1)

return identity`
      });
      this.redis.defineCommand("addMembers", {
        numberOfKeys: 1,
        lua: `local key = KEYS[1]
local members = cjson.decode(ARGV[1])
local added = 0
for _, member in ipairs(members) do
  added = added + redis.call('SADD', key, member)
end
return added`
      });
    }

    async record(event) {
      var _timebase, addKey, bmpKey, cat, dkey, e, idMapKey, j, key, keysQueue, l, labelKey, len, len1, len2, len3, m, n, obj, pipeline, r, ref, ref1, ref2, ref3, ref4, ref5, ref6, ref7, setKey, totKey, valueKey, x, y;
      dkey = this.key + ':' + (_timebase = timebase());
      obj = {...event};
      for (x in obj) {
        for (y in obj) {
          if (x !== y && !x.match(/\~/) && !y.match(/\~/)) {
            cat = [x, y].sort();
            key = cat.join('~');
            if (!obj[key]) {
              obj[key] = obj[cat[0]] + '~' + obj[cat[1]];
            }
          }
        }
      }
      pipeline = this.redis.pipeline();
      keysQueue = [];
      if ((ref = this.map.bmp) != null ? ref.length : void 0) {
        ref1 = this.map.bmp;
        for (j = 0, len = ref1.length; j < len; j++) {
          x = ref1[j];
          if (x.match(/\~/)) {
            x = x.split(/\~/).sort().join('~');
          }
          if (obj[x]) {
            idMapKey = dkey + ':bmp:i:' + x;
            bmpKey = dkey + ':bmp:' + x;
            keysQueue.push(bmpKey);
            // use the command we defined in our pipeline
            pipeline.getIdentityAndSetBit(idMapKey, bmpKey, obj[x]);
          }
        }
      }
      if ((ref2 = this.map.add) != null ? ref2.length : void 0) {
        ref3 = this.map.add;
        for (l = 0, len1 = ref3.length; l < len1; l++) {
          x = ref3[l];
          if (x.match(/\~/)) {
            x = x.split(/\~/).sort().join('~');
          }
          if (obj[x]) {
            addKey = dkey + ':add:' + x;
            keysQueue.push(addKey);
            pipeline.hincrby(addKey, obj[x], 1);
          }
        }
      }
      if ((ref4 = this.map.top) != null ? ref4.length : void 0) {
        ref5 = this.map.top;
        for (m = 0, len2 = ref5.length; m < len2; m++) {
          x = ref5[m];
          if (x.match(/\~/)) {
            x = x.split(/\~/).sort().join('~');
          }
          if (obj[x]) {
            setKey = dkey + ':top:' + x;
            keysQueue.push(setKey);
            pipeline.zincrby(setKey, 1, obj[x]);
          }
        }
      }
      if ((ref6 = this.map.addv) != null ? ref6.length : void 0) {
        ref7 = this.map.addv;
        for (n = 0, len3 = ref7.length; n < len3; n++) {
          x = ref7[n];
          if (!_.isObject(x)) {
            continue;
          }
          labelKey = x.key;
          valueKey = x.addKey;
          if (labelKey.match(/\~/)) {
            labelKey = labelKey.split(/\~/).sort().join('~');
          }
          if (obj[labelKey] && obj[valueKey] && !isNaN(obj[valueKey])) {
            addKey = dkey + ':addv:' + valueKey + ':' + labelKey;
            totKey = dkey + ':addv:' + valueKey + ':' + labelKey + ':i';
            keysQueue.push(addKey);
            keysQueue.push(totKey);
            if (labelKey !== valueKey) {
              pipeline.hincrby(addKey, obj[labelKey], +obj[valueKey]);
            }
            pipeline.hincrby(totKey, 'sum', +obj[valueKey]);
            pipeline.hincrby(totKey, 'count', 1);
          }
        }
      }
      // await @membersKeys.add(_timebase, keysQueue)
      pipeline.addMembers(this.key + ':k:' + _timebase, JSON.stringify(keysQueue));
      try {
        r = (await pipeline.exec());
        return r;
      } catch (error) {
        e = error;
        throw e;
      }
    }

    async query(min, max, opt = {}) {
      var allDays, blacklist, currentDate, dkey, func, item, j, jobKeys, jobPromises, jobs, k, keyPromises, l, len, len1, len2, len3, len4, m, maxDate, minDate, n, p, parts, path, pattern, r, r2, raw, ref, ref1, ref2, results, ret, type, v, valid, x;
      dkey = this.key + ':' + timebase();
      minDate = timebase(min);
      maxDate = timebase(max);
      ret = {};
      keyPromises = [];
      allDays = [];
      currentDate = minDate;
      while (currentDate <= maxDate) {
        keyPromises.push(this.redis.smembers(this.key + ':k:' + currentDate));
        allDays.push(currentDate);
        ret[currentDate] = {
          date: new Date(currentDate * 1000).toISOString().split('T')[0],
          result: []
        };
        currentDate += englishSecs('1 day');
      }
      r = (await Promise.all(keyPromises));
      if (!(r != null ? r.length : void 0)) {
        return ret;
      }
      jobs = this._jobs(r);
      jobPromises = {};
      jobKeys = [];
      blacklist = [];
      for (k in jobs) {
        v = jobs[k];
        for (path in v) {
          func = v[path];
          jobKeys.push(path);
        }
      }
      if (opt.ignore == null) {
        opt.ignore = [];
      }
      if (opt.accept == null) {
        opt.accept = (ref = opt.allow) != null ? ref : [];
      }
      if (opt.ignore) {
        if (typeof opt.ignore === 'string') {
          opt.ignore = [opt.ignore];
        }
      }
      if (opt.accept) {
        if (typeof opt.accept === 'string') {
          opt.accept = [opt.accept];
        }
      }
      if (opt.ignore.length) {
        for (j = 0, len = jobKeys.length; j < len; j++) {
          x = jobKeys[j];
          raw = x.substr(this.key.length + 1);
          parts = raw.split(':');
          parts.shift();
          raw = parts.join(':');
          ref1 = opt.ignore;
          for (l = 0, len1 = ref1.length; l < len1; l++) {
            pattern = ref1[l];
            if (minimatch(raw, pattern)) {
              blacklist.push(x);
            }
          }
        }
      }
      if (opt.accept.length) {
        for (m = 0, len2 = jobKeys.length; m < len2; m++) {
          x = jobKeys[m];
          if (indexOf.call(blacklist, x) >= 0) {
            continue;
          }
          raw = x.substr(this.key.length + 1);
          parts = raw.split(':');
          parts.shift();
          raw = parts.join(':');
          valid = false;
          ref2 = opt.accept;
          for (n = 0, len3 = ref2.length; n < len3; n++) {
            pattern = ref2[n];
            if (minimatch(raw, pattern)) {
              valid = true;
              break;
            }
          }
          if (!valid) {
            blacklist.push(x);
          }
        }
      }
      if (opt.returnJobs) {
        if (blacklist.length) {
          return _.difference(jobKeys, blacklist);
        } else {
          return jobKeys;
        }
      }
      for (k in jobs) {
        v = jobs[k];
        (function(k, v) {
          var k2, len4, p, ref3, v2;
          if (blacklist.length) {
            for (k2 in v) {
              v2 = v[k2];
              if (indexOf.call(blacklist, k2) >= 0) {
                delete v[k2];
              }
            }
          }
          if (opt.ignoreJobs) {
            ref3 = opt.ignoreJobs;
            for (p = 0, len4 = ref3.length; p < len4; p++) {
              x = ref3[p];
              if (k.includes(x)) {
                return;
              }
            }
          }
          return jobPromises[k] = Promise.all(Object.values(v));
        })(k, v);
      }
      r2 = (await Promise.all(Object.values(jobPromises)));
      if (!_.size(r2)) {
        return ret;
      }
      for (type in r2) {
        results = r2[type];
        for (p = 0, len4 = results.length; p < len4; p++) {
          item = results[p];
          if (ret[item.day]) {
            item.key = item.location.split(':').pop().split('~');
            ret[item.day].result.push(item);
          }
        }
      }
      return this._format(ret, false);
    }

    queryDays(numDays) {
      var maxDate, minDate;
      maxDate = timebase();
      minDate = maxDate - (Math.abs(numDays) * (englishSecs('1 day')));
      return this.query(minDate, maxDate);
    }

    async _queryKeys(keys) {
      var days, fns, j, jobPromises, k, l, len, len1, len2, m, mapKey, max, min, out, r, range, start, stats, time, v, x;
      start = new Date();
      min = null;
      max = null;
      for (j = 0, len = keys.length; j < len; j++) {
        x = keys[j];
        time = x.split(':')[1];
        if (!min || time < min) {
          min = time;
        }
        if (!max || time > max) {
          max = time;
        }
      }
      range = (function() {
        var results1 = [];
        for (var l = min; min <= max ? l <= max : l >= max; min <= max ? l++ : l--){ results1.push(l); }
        return results1;
      }).apply(this);
      days = ((function() {
        var l, len1, ref, results1;
        ref = 24 * 60 * 60;
        results1 = [];
        for ((ref > 0 ? (l = 0, len1 = range.length) : l = range.length - 1); ref > 0 ? l < len1 : l >= 0; l += ref) {
          x = range[l];
          results1.push(x);
        }
        return results1;
      })()).reverse();
      fns = this._jobs(keys);
      jobPromises = {};
      for (k in fns) {
        v = fns[k];
        jobPromises[k] = Promise.all(Object.values(v));
      }
      r = (await Promise.all(Object.values(jobPromises)));
      out = {
        days: {},
        min: min,
        max: max,
        elapsed: `${new Date() - start}ms`
      };
      if (_.keys(r).length) {
        for (k in r) {
          v = r[k];
          for (l = 0, len1 = v.length; l < len1; l++) {
            stats = v[l];
            if (!out.days[stats.day]) {
              out.days[stats.day] = {};
            }
            if (!out.days[stats.day][stats.type]) {
              out.days[stats.day][stats.type] = {};
            }
            mapKey = stats.location.split(/:/);
            mapKey = mapKey.slice(-1);
            out.days[stats.day][stats.type][mapKey] = stats;
          }
        }
      }
      for (m = 0, len2 = days.length; m < len2; m++) {
        x = days[m];
        if (!out.days[x]) {
          out.days[x] = {};
        }
        out.days[x].date = new Date(x * 1000).toISOString().split('T')[0];
      }
      return out;
    }

    _jobs(keys) {
      var fns, j, len, y;
      fns = {
        add: {},
        top: {},
        bmp: {},
        addv: {}
      };
      keys = _.uniq(_.flatten(keys));
      for (j = 0, len = keys.length; j < len; j++) {
        y = keys[j];
        ((y) => {
          var addvKey, args, fields, job, key, rest, time, type;
          if (this.key.includes(':')) {
            y = y.split(this.key).join('_tmp_');
          }
          [key, time, type, ...rest] = y.split(/:/);
          if (y.includes('_tmp_')) {
            y = y.split('_tmp_').join(this.key);
          }
          job = {
            day: time,
            type: type,
            location: y
          };
          if (job.type === 'addv') {
            [addvKey, ...fields] = rest;
            job.addvKey = addvKey;
            job.key = fields.join(':').split('~');
          } else {
            job.key = rest.join(':').split('~');
          }
          if (job.type === 'add') {
            return fns[job.type][job.location] = this.redis.hgetall(job.location).then(function(r) {
              job.result = r;
              return job;
            });
          } else if (job.type === 'addv') {
            if (job.key.includes('i')) {
              // Handle the summary document
              return fns[job.type][job.location] = this.redis.hgetall(job.location).then(function(r) {
                job.result = {
                  sum: Number(r.sum),
                  count: Number(r.count)
                };
                return job;
              });
            } else {
              // Handle the main addv document
              return fns[job.type][job.location] = this.redis.hgetall(job.location).then(function(r) {
                var k, v;
                job.result = {};
                for (k in r) {
                  v = r[k];
                  job.result[k] = Number(v);
                }
                return job;
              });
            }
          } else if (job.type === 'bmp') {
            return fns[job.type][job.location] = this.redis.bitcount(job.location).then(function(r) {
              job.result = r;
              return job;
            });
          } else if (job.type === 'top') {
            args = [job.location, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0, 250];
            return fns[job.type][job.location] = this.redis.zrevrangebyscore(args).then(function(r) {
              var i, l, last, len1, ret, z;
              ret = {};
              if (r != null ? r.length : void 0) {
                last = null;
                i = 0;
                for (l = 0, len1 = r.length; l < len1; l++) {
                  z = r[l];
                  ++i;
                  if (i % 2) {
                    ret[z] = null;
                    last = z;
                  } else {
                    ret[last] = parseInt(z);
                  }
                }
              }
              job.result = ret;
              return job;
            });
          }
        })(y);
      }
      return fns;
    }

    x_jobs(keys) {
      var fns, j, len, y;
      fns = {
        add: {},
        top: {},
        bmp: {},
        addv: {}
      };
      keys = _.uniq(_.flatten(keys));
      for (j = 0, len = keys.length; j < len; j++) {
        y = keys[j];
        ((y) => {
          var args, fields, job, key, ref, time, type;
          if (this.key.includes(':')) {
            y = y.split(this.key).join('_tmp_');
          }
          ref = y.split(/:/), [key, time, ...type] = ref, [fields] = splice.call(type, -1);
          if (y.includes('_tmp_')) {
            y = y.split('_tmp_').join(this.key);
          }
          if (_.first(type) === 'addv') {
            type = ['addv'];
          }
          if (type.length === 2) {
            return;
          }
          job = {
            day: time,
            type: type.shift(),
            location: y
          };
          if (job.type === 'add' || job.type === 'addv') {
            return fns[job.type][job.location] = this.redis.hgetall(job.location).then(function(r) {
              job.result = r;
              return job;
            });
          } else if (job.type === 'bmp') {
            return fns[job.type][job.location] = this.redis.bitcount(job.location).then(function(r) {
              job.result = r;
              return job;
            });
          } else if (job.type === 'top') {
            args = [job.location, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0, 250];
            return fns[job.type][job.location] = this.redis.zrevrangebyscore(args).then(function(r) {
              var i, l, last, len1, ret, z;
              ret = {};
              if (r != null ? r.length : void 0) {
                last = null;
                i = 0;
                for (l = 0, len1 = r.length; l < len1; l++) {
                  z = r[l];
                  ++i;
                  if (i % 2) {
                    ret[z] = null;
                    last = z;
                  } else {
                    ret[last] = parseInt(z);
                  }
                }
              }
              job.result = ret;
              return job;
            });
          }
        })(y);
      }
      return fns;
    }

    _format(obj) {
      var mergeNumeric, output;
      mergeNumeric = (function(uno, dos) {
        var k, v;
        if (!uno && dos) {
          return dos;
        }
        if (uno && !dos) {
          return uno;
        }
        for (k in dos) {
          v = dos[k];
          if (uno[k] == null) {
            uno[k] = 0;
          }
          uno[k] += +v;
        }
        return uno;
      });
      return output = {
        days: obj,
        find: function(o) {
          var arr, item, j, l, len, len1, len2, m, opt, parts, ref, ref1, ref2, ref3, ref4, ret, tot, unix, v, val, x;
          opt = {
            type: null,
            key: null,
            addKey: null,
            day: false,
            merge: false
          };
          if (typeof o === 'object') {
            if (o.keys && !o.key) {
              o.key = o.keys;
              delete o.keys;
            }
            opt = _.merge({}, opt, o);
          } else {
            parts = o.split('/');
            opt.type = parts.shift();
            opt.key = parts.shift();
            if (parts.length) {
              opt.day = parts.shift();
            }
          }
          opt.key = opt.key.split('~').sort().join('~');
          if (opt.day) {
            if (((ref = obj[opt.day]) != null ? ref.result : void 0) == null) {
              return null;
            }
            ref1 = obj[opt.day].result;
            for (j = 0, len = ref1.length; j < len; j++) {
              v = ref1[j];
              if (v.type === opt.type) {
                if (v.type === 'addv') {
                  if (v.addvKey === opt.addKey && v.key.sort().join('~') === opt.key) {
                    return v.result;
                  }
                } else if (v.location.substr((opt.key.length + 1) * -1) === `:${opt.key}`) {
                  return v.result;
                }
              }
            }
            return null;
          } else {
            ret = {};
            for (unix in obj) {
              item = obj[unix];
              if (opt.type === 'bmp') {
                val = 0;
              } else if ((ref2 = opt.type) === 'top' || ref2 === 'add' || ref2 === 'addv') {
                val = {};
              }
              ret[item.date] = val;
              if (!(item != null ? (ref3 = item.result) != null ? ref3.length : void 0 : void 0)) {
                continue;
              }
              ref4 = item.result;
              for (l = 0, len1 = ref4.length; l < len1; l++) {
                v = ref4[l];
                if (v.type === opt.type) {
                  if (v.type === 'addv') {
                    if (v.addvKey === opt.addKey && v.key.sort().join('~') === opt.key) {
                      ret[item.date] = v.result;
                    }
                  } else if (v.location.substr((opt.key.length + 1) * -1) === `:${opt.key}`) {
                    ret[item.date] = v.result;
                  }
                }
              }
            }
            if (opt.merge) {
              tot = {};
              arr = _.values(ret);
              for (m = 0, len2 = arr.length; m < len2; m++) {
                x = arr[m];
                (function(x) {
                  return tot = mergeNumeric(tot, x);
                })(x);
              }
              return tot;
            } else {
              return ret;
            }
          }
        }
      };
    }

  };

  if (!module.parent) {
    process.exit(0);
  }

}).call(this);
