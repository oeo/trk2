# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2

cache = null

module.exports = (client, key = 'id-map') ->
  script = """
    local identity = redis.call("ZSCORE", KEYS[1], ARGV[1])
    if not identity then
      identity = redis.call("ZCARD", KEYS[1])
      redis.call("ZADD", KEYS[1], identity, ARGV[1])
    end
    return identity
  """

  (id) ->
    throw new Error "`id` required" if !id

    evalSha = ->
      result = await client.evalsha(cache, 1, key, id)
      parseInt(result, 10)

    if !cache
      cache = await client.send_command('SCRIPT', ['LOAD', script])

    await evalSha()

##
if !module.parent
  do ->
    Redis = require('ioredis')
    client = new Redis()

    idMap = module.exports(client)

    try
      result1 = await idMap('user1')
      console.log('Result for user1:', result1)

      result2 = await idMap('user2')
      console.log('Result for user2:', result2)

      result3 = await idMap('user1')
      console.log('Result for user1 again:', result3)
    catch err
      console.error('Error:', err)
    finally
      await client.quit()
      process.exit(0)

