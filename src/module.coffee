# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2

_ = require 'lodash'
Redis = require 'ioredis'
minimatch = require 'minimatch'

englishSecs = require './english-secs'
timebase = require './timebase'

module.exports = class Metrics

  constructor: (opt={}) ->
    @redis = opt.redis ? opt.client ? new Redis()
    @key = opt.key ? opt.prefix ? 'trk2'

    @map = {
      bmp: []
      add: []
      addv: []
      top: []
    }

    if opt.map
      @map[k] = v for k,v of opt.map

    @redis.defineCommand("getIdentityAndSetBit", {
      numberOfKeys: 2,
      lua: """
        local idMapKey = KEYS[1]
        local bmpKey = KEYS[2]
        local id = ARGV[1]

        local identity = redis.call("ZSCORE", idMapKey, id)
        if not identity then
          identity = redis.call("ZCARD", idMapKey)
          redis.call("ZADD", idMapKey, identity, id)
        end

        redis.call("SETBIT", bmpKey, identity, 1)

        return identity
      """
    })

    @redis.defineCommand("addMembers", {
      numberOfKeys: 1,
      lua: """
        local key = KEYS[1]
        local members = cjson.decode(ARGV[1])
        local added = 0
        for _, member in ipairs(members) do
          added = added + redis.call('SADD', key, member)
        end
        return added
      """
    })

  record: (event) ->
    dkey = @key + ':' + (_timebase = timebase())
    obj = {...event}

    for x of obj
      for y of obj
        if x != y and !x.match(/\~/) and !y.match(/\~/)
          cat = [x,y].sort()
          key = cat.join('~')
          if not obj[key]
            obj[key] = obj[cat[0]] + '~' + obj[cat[1]]

    pipeline = @redis.pipeline()

    keysQueue = []

    if @map.bmp?.length
      for x in @map.bmp
        if x.match(/\~/)
          x = x.split(/\~/).sort().join('~')

        if obj[x]
          idMapKey = dkey + ':bmp:i:' + x
          bmpKey = dkey + ':bmp:' + x

          keysQueue.push bmpKey

          # use the command we defined in our pipeline
          pipeline.getIdentityAndSetBit(idMapKey, bmpKey, obj[x])

    if @map.add?.length
      for x in @map.add
        if x.match(/\~/)
          x = x.split(/\~/).sort().join('~')

        if obj[x]
          addKey = dkey + ':add:' + x
          keysQueue.push addKey
          pipeline.hincrby(addKey, obj[x], 1)

    if @map.top?.length
      for x in @map.top
        if x.match(/\~/)
          x = x.split(/\~/).sort().join('~')

        if obj[x]
          setKey = dkey + ':top:' + x
          keysQueue.push setKey
          pipeline.zincrby(setKey, 1, obj[x])

    if @map.addv?.length
      for x in @map.addv

        # @todo: enforce this structure {key, value}
        if !_.isObject(x)
          continue

        labelKey = x.key
        valueKey = x.addKey

        if labelKey.match(/\~/)
          labelKey = labelKey.split(/\~/).sort().join('~')

        if obj[labelKey] and obj[valueKey] and !isNaN(obj[valueKey])
          addKey = dkey + ':addv:' + valueKey + ':' + labelKey
          totKey = dkey + ':addv:' + valueKey + ':' + labelKey + ':i'

          keysQueue.push addKey
          keysQueue.push totKey

          if labelKey isnt valueKey
            pipeline.hincrby(addKey, obj[labelKey], +(obj[valueKey]))

          pipeline.hincrby(totKey, 'sum', +(obj[valueKey]))
          pipeline.hincrby(totKey, 'count', 1)

    # await @membersKeys.add(_timebase, keysQueue)
    pipeline.addMembers(@key + ':k:' + _timebase, JSON.stringify(keysQueue))

    try
      r = await pipeline.exec()
      return r
    catch e
      throw e

  query: (min, max, opt = {}) ->
    dkey = @key + ':' + timebase()

    minDate = timebase(min)
    maxDate = timebase(max)

    ret = {}

    keyPromises = []
    allDays = []

    currentDate = minDate

    while currentDate <= maxDate
      keyPromises.push(@redis.smembers(@key + ':k:' + currentDate))
      allDays.push currentDate

      ret[currentDate] = {
        date: new Date(currentDate * 1000).toISOString().split('T')[0]
        result: []
      }

      currentDate += englishSecs('1 day')

    r = await Promise.all(keyPromises)

    if !r?.length
      return ret

    jobs = @_jobs(r)
    jobPromises = {}

    jobKeys = []
    blacklist = []

    for k,v of jobs
      for path, func of v
        jobKeys.push path

    opt.ignore ?= []
    opt.accept ?= (opt.allow ? [])

    if opt.ignore
      opt.ignore = [opt.ignore] if typeof opt.ignore is 'string'

    if opt.accept
      opt.accept = [opt.accept] if typeof opt.accept is 'string'

    if opt.ignore.length
      for x in jobKeys
        raw = x.substr(@key.length + 1)
        parts = raw.split ':'
        parts.shift()
        raw = parts.join ':'
        for pattern in opt.ignore
          blacklist.push(x) if minimatch(raw, pattern)

    if opt.accept.length
      for x in jobKeys
        continue if x in blacklist
        raw = x.substr(@key.length + 1)
        parts = raw.split ':'
        parts.shift()
        raw = parts.join ':'
        valid = no
        for pattern in opt.accept
          if minimatch(raw, pattern)
            valid = yes
            break
        blacklist.push x if !valid

    if opt.returnJobs
      if blacklist.length
        return _.difference(jobKeys, blacklist)
      else
        return jobKeys

    for k,v of jobs
      do (k,v) ->
        if blacklist.length
          for k2,v2 of v
            delete v[k2] if k2 in blacklist

        if opt.ignoreJobs
          for x in opt.ignoreJobs
            return if k.includes(x)

        jobPromises[k] = Promise.all(Object.values(v))

    r2 = await Promise.all(Object.values(jobPromises))

    if !_.size(r2)
      return ret

    for type, results of r2
      for item in results
        if ret[item.day]
          item.key = item.location.split(':').pop().split('~')
          ret[item.day].result.push item

    @_format(ret, no)

  queryDays: (numDays) ->
    maxDate = timebase()
    minDate = maxDate - (Math.abs(numDays) * (englishSecs('1 day')))

    @query(minDate, maxDate)

  _queryKeys: (keys) ->
    start = new Date

    min = null
    max = null

    for x in keys
      time = x.split(':')[1]
      if !min or time < min then min = time
      if !max or time > max then max = time

    range = [min..max]
    days = (x for x in range by (24 * 60 * 60)).reverse()

    fns = @_jobs keys
    jobPromises = {}

    for k,v of fns
      jobPromises[k] = Promise.all(Object.values(v))

    r = await Promise.all(Object.values(jobPromises))

    out = {
      days: {}
      min: min
      max: max
      elapsed: "#{new Date() - start}ms"
    }

    if _.keys(r).length
      for k,v of r
        for stats in v

          if !out.days[stats.day]
            out.days[stats.day] = {}

          if !out.days[stats.day][stats.type]
            out.days[stats.day][stats.type] = {}

          mapKey = stats.location.split /:/
          mapKey = mapKey.slice -1

          out.days[stats.day][stats.type][mapKey] = stats

    for x in days
      if !out.days[x] then out.days[x] = {}
      out.days[x].date = new Date(x * 1000).toISOString().split('T')[0]

    out

  _jobs: (keys) ->
    fns =
      add: {}
      top: {}
      bmp: {}
      addv: {}

    keys = _.uniq _.flatten keys

    for y in keys
      do (y) =>
        if @key.includes(':')
          y = y.split(@key).join('_tmp_')

        [key, time, type, rest...] = y.split /:/

        if y.includes('_tmp_')
          y = y.split('_tmp_').join @key

        job =
          day: time
          type: type
          location: y

        if job.type is 'addv'
          [addvKey, ...fields] = rest
          job.addvKey = addvKey
          job.key = fields.join(':').split('~')
        else
          job.key = rest.join(':').split('~')

        if job.type is 'add'
          fns[job.type][job.location] = @redis.hgetall(job.location).then (r) ->
            job.result = r
            job

        else if job.type is 'addv'
          if job.key.includes('i')
            # Handle the summary document
            fns[job.type][job.location] = @redis.hgetall(job.location).then (r) ->
              job.result =
                sum: Number(r.sum)
                count: Number(r.count)
              job
          else
            # Handle the main addv document
            fns[job.type][job.location] = @redis.hgetall(job.location).then (r) ->
              job.result = {}
              for k, v of r
                job.result[k] = Number(v)
              job

        else if job.type is 'bmp'
          fns[job.type][job.location] = @redis.bitcount(job.location).then (r) ->
            job.result = r
            job

        else if job.type is 'top'
          args = [
            job.location
            '+inf'
            '-inf'
            'WITHSCORES'
            'LIMIT'
            0
            250
          ]

          fns[job.type][job.location] = @redis.zrevrangebyscore(args).then (r) ->
            ret = {}
            if r?.length
              last = null
              i = 0
              for z in r
                ++i
                if i % 2
                  ret[z] = null
                  last = z
                else
                  ret[last] = parseInt z
            job.result = ret
            job

    fns

  x_jobs: (keys) ->
    fns =
      add: {}
      top: {}
      bmp: {}
      addv: {}

    keys = _.uniq _.flatten keys

    for y in keys
      do (y) =>
        if @key.includes(':')
          y = y.split(@key).join('_tmp_')

        [key, time, type..., fields] = y.split /:/

        if y.includes('_tmp_')
          y = y.split('_tmp_').join @key

        if _.first(type) is 'addv'
          type = ['addv']

        return if type.length is 2

        job =
          day: time
          type: type.shift()
          location: y

        if job.type is 'add' or job.type is 'addv'
          fns[job.type][job.location] = @redis.hgetall(job.location).then (r) ->
            job.result = r
            job

        else if job.type is 'bmp'
          fns[job.type][job.location] = @redis.bitcount(job.location).then (r) ->
            job.result = r
            job

        else if job.type is 'top'
          args = [
            job.location
            '+inf'
            '-inf'
            'WITHSCORES'
            'LIMIT'
            0
            250
          ]

          fns[job.type][job.location] = @redis.zrevrangebyscore(args).then (r) ->
            ret = {}
            if r?.length
              last = null
              i = 0
              for z in r
                ++i
                if i % 2
                  ret[z] = null
                  last = z
                else
                  ret[last] = parseInt z
            job.result = ret
            job

    return fns

  _format: (obj) ->
    mergeNumeric = ((uno, dos) ->
      return dos if !uno and dos
      return uno if uno and !dos

      for k,v of dos
        uno[k] ?= 0
        uno[k] += (+v)

      return uno
    )

    output =
      days: obj
      find: (o) ->
        opt = {
          type: null

          key: null
          addKey: null

          day: no
          merge: no
        }

        if typeof o is 'object'
          if o.keys and !o.key
            o.key = o.keys
            delete o.keys
          opt = _.merge {}, opt, o
        else
          parts = o.split '/'
          opt.type = parts.shift()
          opt.key = parts.shift()
          if parts.length
            opt.day = parts.shift()

        opt.key = opt.key.split('~').sort().join '~'

        if opt.day
          if !obj[opt.day]?.result?
            return null

          for v in obj[opt.day].result
            if v.type is opt.type
              if v.type is 'addv'
                if v.addvKey is opt.addKey and v.key.sort().join('~') is opt.key
                  return v.result
              else if v.location.substr((opt.key.length + 1) * -1) is ":#{opt.key}"
                return v.result

          return null

        else
          ret = {}

          for unix, item of obj
            if opt.type is 'bmp'
              val = 0
            else if opt.type in ['top','add','addv']
              val = {}

            ret[item.date] = val

            continue if !item?.result?.length

            for v in item.result
              if v.type is opt.type
                if v.type is 'addv'
                  if v.addvKey is opt.addKey and v.key.sort().join('~') is opt.key
                    ret[item.date] = v.result
                else if v.location.substr((opt.key.length + 1) * -1) is ":#{opt.key}"
                  ret[item.date] = v.result

          if opt.merge
            tot = {}
            arr = _.values ret
            for x in arr
              do (x) ->
                tot = mergeNumeric tot, x
            tot
          else
            ret

if !module.parent
  process.exit 0

