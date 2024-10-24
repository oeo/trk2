# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
moment = require 'moment-timezone'

DEFAULT_TIMEZONE = 'America/New_York'

timebase = (input, options = {}) ->
  if typeof input is 'object'
    options = input
    input = undefined

  defaults = {
    startPoint: 'day'
    format: 'unixSeconds'
    timezone: DEFAULT_TIMEZONE
  }

  for k, v of defaults
    options[k] ?= v

  # handle input
  if input is undefined
    date = moment().tz(options.timezone)
  else if typeof input is 'number'
    date = moment.unix(input).tz(options.timezone)
  else if typeof input is 'string'
    date = moment.tz(input, options.timezone)
  else if input instanceof Date
    date = moment(input).tz(options.timezone)
  else if typeof input is 'object' and not (input instanceof Date)
    options = input
    date = moment().tz(options.timezone)
  else
    throw new Error('Invalid input type: ' + typeof input)

  # adjust to start of period
  switch options.startPoint
    when 'minute'
      date.startOf('minute')
    when 'hour'
      date.startOf('hour')
    when 'day'
      date.startOf('day')
    when 'month'
      date.startOf('month')
    when 'year'
      date.startOf('year')
    else
      throw new Error('Invalid startPoint')

  # format output
  switch options.format
    when 'unixSeconds'
      date.unix()
    when 'unixMilliseconds'
      date.valueOf()
    when 'date'
      date.toDate()
    else
      throw new Error('Invalid format')

timebase.DEFAULT_TIMEZONE = DEFAULT_TIMEZONE

##
module.exports = timebase

if !module.parent
  console.log "Today usage one", timebase()
  console.log "Today usage two", timebase({timezone: 'UTC'})

  # get the start of the current minute
  console.log "Minute", timebase(startPoint: 'minute')

  # get the start of the current hour
  console.log "Hour", timebase(startPoint: 'hour')

  # get the start of the current month
  console.log "Month", timebase(startPoint: 'month')

  # get the start of the current year
  console.log "Year", timebase(startPoint: 'year')

  console.log "Today, different format", timebase('2023-05-15T14:30:00Z')

  # get the start of the month as a date object
  console.log "No param passed for time, return as date", timebase(startPoint: 'year', format: 'date')

