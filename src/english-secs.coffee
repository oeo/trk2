# vim: set expandtab tabstop=2 shiftwidth=2 softtabstop=2
milliseconds = (n) -> n
x = (fn, multiples) -> (n) -> fn(n) * multiples

seconds = x(milliseconds, 1000)
minutes = x(seconds, 60)
hours = x(minutes, 60)
days = x(hours, 24)
weeks = x(days, 7)
months = x(days, 31)
years = x(months, 12)

units =
  ms: milliseconds
  millisec: milliseconds
  millisecs: milliseconds
  millisecond: milliseconds
  milliseconds: milliseconds
  s: seconds
  sec: seconds
  secs: seconds
  seconds: seconds
  second: seconds
  m: minutes
  min: minutes
  mins: minutes
  minute: minutes
  minutes: minutes
  h: hours
  hr: hours
  hrs: hours
  hour: hours
  hours: hours
  d: days
  day: days
  days: days
  w: weeks
  wk: weeks
  wks: weeks
  week: weeks
  weeks: weeks
  mo: months
  mos: months
  month: months
  months: months
  y: years
  yr: years
  yrs: years
  year: years
  years: years

# rewrite
singular = [
  'millisecond'
  'second'
  'minute'
  'hour'
  'day'
  'week'
  'month'
  'year'
]

numbers = [
  'zero'
  'one'
  'two'
  'three'
  'four'
  'five'
  'six'
  'seven'
  'eight'
  'nine'
  'ten'
  'eleven'
  'twelve'
]

rewrite = (input, customSingular, customNumbers) ->
  re = new RegExp("((?:[a-z]\\s)|^)(#{(customNumbers or numbers).join('|')})(?=\\s|$)", 'g')
  output = input.replace re, (_, prefix, n) ->
    value = numbers.indexOf(n)
    prefix + (if value == -1 then n else value)

  re = new RegExp("((?:[a-z]\\s)|^)(#{(customSingular or singular).join('|')})(?=\\s|$)", 'g')
  output = output.replace re, (_, prefix, unit) ->
    "#{prefix}1 #{unit}"

  output

# parse
reOne = /(\d+)\s?(\w+)/
reAll = /(\d+\s*\w+)/g

one = (input, units) ->
  [_, n, unit] = input.match(reOne)
  if units[unit]
    units[unit](parseInt(n))

all = (input, units) ->
  input = input.toLowerCase()
  unless reAll.test(input) and /\d+[\w\s,]+/.test(input) and /\w$/.test(input)
    throw new Error("Invalid time: \"#{input}\"")

  allMatches = input.match(reAll)
  allMatches = allMatches.map((m) -> one(m, units)).filter((el) -> el?)

  if allMatches.length == 0
    return

  allMatches.reduce((a, b) -> a + b)

# primary export
module.exports = etime = (input, options = {}) ->
  return input if typeof input == 'number'
  result = all(rewrite(input, options.customSingularUnits), options.customUnits or units)
  if options.milliseconds
    result
  else
    Math.floor(result / 1000)

module.exports.units = units

if !module.parent
  console.log etime('1 hr')  # Outputs seconds
  console.log etime('1 mos')  # Outputs seconds
  console.log etime('1 month')  # Outputs seconds
  console.log etime('1 hr', {milliseconds: true})  # Outputs milliseconds
  console.log etime('1 mos', {milliseconds: true})  # Outputs milliseconds
  console.log etime('1 month', {milliseconds: true})  # Outputs milliseconds
  console.log etime('1 month')

