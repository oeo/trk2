# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
_ = require 'lodash'
Redis = require 'ioredis'
moment = require 'moment'
{ promisify } = require 'util'

class Metrics
  constructor: (options = {}) ->
    @redis = options.redis ? new Redis(options.redisUrl)
    @key = options.key ? 'metrics'

    @map = {
      bmp: []
      add: []
      addv: []
      top: []
    }

    @map = {...@map, ...options.map} if options.map

  record: (event) ->
    dkey = "#{@key}:#{moment().format('YYYYMMDD')}"
    obj = _.cloneDeep(event)

    # combine and sort keys
    for x of obj
      for y of obj
        if x isnt y and not x.includes('~') and not y.includes('~')
          cat = [x, y].sort()
          key = cat.join('~')
          if not obj[key]?
            obj[key] = "#{obj[cat[0]]}~#{obj[cat[1]]}"

    pipeline = @redis.pipeline()

    # process each map type
    for mapType, keys of @map
      for x in keys

        # sort compound keys
        if x.includes('~')
          x = x.split('~').sort().join('~')

        if obj[x]?
          switch mapType
            when 'bmp'
              bmpKey = "#{dkey}:bmp:#{x}"
              pipeline.pfadd(bmpKey, obj[x])
            when 'add'
              addKey = "#{dkey}:add:#{x}"
              pipeline.hincrby(addKey, obj[x], 1)
            when 'top'
              topKey = "#{dkey}:top:#{x}"
              pipeline.zincrby(topKey, 1, obj[x])
            when 'addv'
              if not isNaN(obj[x])
                addvKey = "#{dkey}:addv:#{x}:#{x}"
                totKey = "#{dkey}:addv:#{x}:i"
                pipeline.hincrby(addvKey, obj[x], +obj[x])
                pipeline.hincrby(totKey, 'sum', +obj[x])
                pipeline.hincrby(totKey, 'count', 1)

    await pipeline.exec()

  query: (min, max, options = {}) ->
    minDate = moment.unix(min).startOf('day')
    maxDate = moment.unix(max).startOf('day')
    days = maxDate.diff(minDate, 'days') + 1

    results = {}

    for i in [0...days]
      date = minDate.clone().add(i, 'days')
      unix = date.unix()
      dkey = "#{@key}:#{date.format('YYYYMMDD')}"

      # @todo: implement cache get here

      result = await @queryDay(dkey, unix, options)
      results[unix] = result

    @formatResults(results, options)

  queryDay: (dkey, unix, options) ->

    # @todo: implement cache get here

    pipeline = @redis.pipeline()

    for type in ['bmp', 'add', 'top', 'addv']
      for x in @map[type]

        key = "#{dkey}:#{type}:#{x}"

        switch type
          when 'bmp' then pipeline.pfcount(key)
          when 'add' then pipeline.hgetall(key)
          when 'top' then pipeline.zrevrange(key, 0, -1, 'WITHSCORES')
          when 'addv'
            pipeline.hgetall("#{key}:#{x}")
            pipeline.hgetall("#{key}:i")

    results = await pipeline.exec()
    formattedResults = @formatDayResults(results, unix)

    # @todo: implement cache put here

    return formattedResults

  formatDayResults: (results, unix) ->
    formattedResults = {unix, date: moment.unix(unix).format('YYYY-MM-DD')}

    index = 0
    for type in ['bmp', 'add', 'top', 'addv']
      for x in @map[type]

        key = "#{type}:#{x}"

        switch type
          when 'bmp'
            formattedResults[key] = results[index][1]
            index++
          when 'add', 'top'
            formattedResults[key] = results[index][1]
            index++
          when 'addv'
            values = results[index][1]
            totals = results[index + 1][1]
            formattedResults[key] = { values, totals }
            index += 2

    return formattedResults

  formatResults: (results, options) ->
    formatted = { days: results }

    # define subquery function
    formatted.find = (query) =>
      if typeof query is 'string'
        [type, key, day] = query.split('/')
      else
        {type, key, day, merge} = query
      if day
        return results[day]?["#{type}:#{key}"]
      else
        dayResults = {}
        for unix, dayData of results
          dayResults[dayData.date] = dayData["#{type}:#{key}"]
        if merge
          mergedResult = {}
          for date, data of dayResults
            for k, v of data
              mergedResult[k] ?= 0
              mergedResult[k] += +v
          return mergedResult
        else
          return dayResults

    return formatted

  # ease of use function
  queryDays: (numDays) ->
    max = moment().unix()
    min = moment().subtract(Math.abs(numDays), 'days').unix()
    @query(min, max)

module.exports = Metrics

if !module.parent
  metrics = new Metrics({
    key: 'examples'

    # define the map
    map: {
      bmp: ['ip']
      add: [
        'event'
        'event~offer'
        'event~offer~creative'
        'event~offer~channel'
        'event~offer~s1'
        'event~offer~s2'
        'event~offer~s3'
        'event~offer~creative~s1'
        'event~offer~creative~s2'
        'event~offer~creative~s3'
      ]
      top: [
        'geo'
        'offer'
        'geo~offer'
        'offer~creative'
        'offer~host'
        'offer~ref'
      ]
    }
  })

  # record an event
  await metrics.record({
    ip: '192.168.1.1'
    event: 'offer_impression'
    offer: '526aa9fff3e8b600000000e5'
    creative: 'c_0'
    geo: 'US'
    host: 'example.com'
    ref_host: 'google.com'
  })

  # query the last 10 days
  results = await metrics.queryDays(10)
  console.log(results.find('bmp/ip'))
  console.log(results.find({type: 'add', key: 'event', merge: true}))
  console.log(results.find('top/geo'))

