# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
_ = require 'lodash'
Redis = require 'ioredis'

Metrics = require './../module'

redis = new Redis()

metrics = new Metrics {
  redis: redis
  key: 'examples'
  map: {
    bmp: [
      'ip'
    ]
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
    addv: [
      { key: 'offer~event', addKey: 'amount' }
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
}

# subfunctions to generate random event properties
randomIp = -> [_.random(1,128), _.random(0,255), _.random(0,255), _.random(0,255)].join('.')
randomArr = (a) -> _.sample(a)

events = [
  'offer_impression'
  'offer_impression'
  'offer_impression'
  'offer_click'
  'offer_conversion'
]

offers = [
  '526aa9fff3e8b600000000e5'
  '526aa9fff3e8b60000000002'
  '526aa9fff3e8b6000000000b'
  '526aa9fff3e8b6000000000b'
  '526aa9fff3e8b6000000000b'
]

creatives = ['c_0','c_1','c_2']

domains = [
  'aol.com'
  'google.com'
  'gmail.com'
  'hotmail.com'
  'example.com'
]

# store events in an array before recording
data = []

numEvents = 10000

for x in [1..numEvents]
  data.push {
    ip: randomIp()
    event: randomArr(events)
    geo: randomArr(['US','US','US','UK'])
    chan: randomArr(['any','text','text'])
    offer: randomArr(offers)
    creative: randomArr(creatives)
    ua: 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36'
    host: randomArr(domains)
    ref_host: randomArr(domains)
    amount: _.random(1,100)
  }

do ->
  # "synchronous", record events one after the other
  start = new Date()

  for eventObj in data
    await metrics.record(eventObj)

  elapsed = new Date() - start
  console.log "Finished recording #{numEvents} events (series) in #{elapsed}ms"
  console.log "Series events digested/sec: #{numEvents/(elapsed/1000)}"

  # parallel, record X at a time
  start = new Date()

  await Promise.all(data.map((eventObj) -> metrics.record(eventObj)))

  elapsed = new Date() - start
  console.log "Finished recording #{numEvents} events (parallel) in #{elapsed}ms"
  console.log "Parallel events digested/sec: #{numEvents/(elapsed/1000)}"

  process.exit(0)

