# trk2

a lightweight metrics tracking library for redis

## installation

```bash
npm install trk2 --save
```

## usage

### basic setup

```javascript
const Redis = require('ioredis');
const Metrics = require('trk2');

const redis = new Redis();

const metrics = new Metrics({
  redis: redis,
  key: 'myapp',
  map: {
    bmp: ['ip'],
    add: ['event', 'event~offer'],
    addv: [{ key: 'offer~event', addKey: 'amount' }],
    top: ['geo', 'offer']
  }
});
```

### recording events

```javascript
await metrics.record({
  ip: '192.168.1.1',
  event: 'offer_impression',
  offer: '123456',
  geo: 'US',
  amount: 100
});
```

### querying data

```javascript
// query last 10 days
const results = await metrics.queryDays(10);

// get unique visitor counts
const unique = results.find({
  type: 'bmp',
  key: 'ip'
});

// get add value for amount
const addValue = results.find({
  type: 'addv',
  key: 'offer~event',
  addKey: 'amount'
});

// get event counts
const impressions = results.find({
  type: 'add',
  key: 'event',
  merge: false
});

// get top geos
const topGeos = results.find('top/geo');

// get top offers
const topOffers = results.find('top/offer');
```

## api

### `new Metrics(options)`

creates a new metrics instance.

- `options.redis`: redis client instance
- `options.key`: prefix for redis keys
- `options.map`: configuration for tracking different metrics types

### `metrics.record(event)`

records a single event.

### `metrics.queryDays(numDays)`

queries data for the last `numDays` days.

### `results.find(options)`

finds specific metrics in the query results.

- `options.type`: type of metric ('bmp', 'add', 'addv', 'top')
- `options.key`: key for the metric
- `options.addKey`: additional key for 'addv' type
- `options.merge`: whether to merge results across days

## examples

see the `src/examples` directory for more detailed usage examples.

## performance
on my apple m2 max w 64gb ram

coffee/node

```bash
taky:~/www/trk2/src$ coffee examples/record-events.coffee

Finished recording 100000 events (series) in 12799ms
Series events digested/sec: 7813.110399249942

Finished recording 100000 events (parallel) in 10263ms
Parallel events digested/sec: 9743.739647276625
```

bun

```bash
taky:~/www/trk2/build$ bun examples/record-events.js

Finished recording 100000 events (series) in 10294ms
Series events digested/sec: 9714.396735962697

Finished recording 100000 events (parallel) in 7499ms
Parallel events digested/sec: 13335.111348179758
```

## license

mit

