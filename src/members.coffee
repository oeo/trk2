# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
_ = require 'lodash'
cache = require 'memory-cache'
crypto = require 'crypto'
Redis = require 'ioredis'

etime = require './english-secs'

class RedisMembers
  constructor: (opts = {}) ->
    @redis = opts.redis ? new Redis(6379, 'localhost')
    @prefix = opts.prefix ? 'members'
    @opts = {
      trimValues: opts.trimValues ? yes
      hashGroupNames: opts.hashGroupNames ? no
      cacheTime: etime(opts.cacheTime ? '10 minutes') * 1000
    }

  add: (groupName, member, cb) ->
    key = @_getKey(groupName)

    if typeof member is 'string'
      member = @_trim(member)
      cacheKey = key + member

      if not cache.get(cacheKey)
        try
          r = await @redis.sadd(key, member)
          cache.put(cacheKey, yes, @opts.cacheTime)
          return r
        catch e
          throw e

    else if Array.isArray(member)
      pipeline = @redis.pipeline()

      for x in member
        x = @_trim(x.toString())
        cacheKey = key + x

        if not cache.get(cacheKey)
          pipeline.sadd(key, x)
          cache.put(cacheKey, yes, @opts.cacheTime)

      try
        r = await pipeline.exec()
        return r
      catch e
        throw e

  remove: (groupName, member, cb) ->
    key = @_getKey(groupName)
    members = if typeof member is 'string' then [member] else member

    pipeline = @redis.pipeline()

    for x in members
      x = @_trim(x)
      cacheKey = key + x
      cache.del(cacheKey)
      pipeline.srem(key, x)

    try
      r = await pipeline.exec()
      return members.length
    catch e
      throw e

  list: (groupName, cb) ->
    key = @_getKey(groupName)

    try
      members = await @redis.smembers(key)
      return members
    catch e
      throw e

  _getKey: (groupName) ->
    if @opts.hashGroupNames
      "#{@prefix}:#{@_sha(groupName)}:"
    else
      "#{@prefix}:#{groupName}:"

  _trim: (value) ->
    if @opts.trimValues then value.trim() else value

  _sha: (str) ->
    crypto.createHash('sha256').update(str).digest('hex')

module.exports = RedisMembers

if !module.parent
  log = (x) -> try console.log x

  m = new RedisMembers()

  do ->
    await m.add 'friends', ['james', 'john', 'jerry']
    await m.add 'friends', 'john'

    friends = await m.list 'friends'

    log /friends before removal/
    log friends

    await m.remove 'friends', ['james', 'john', 'noexists']

    friends = await m.list 'friends', (e, r) -> if e then reject(e) else resolve(r)

    log /friends after removal/
    log friends

    process.exit 1

