# Arb History CSV creator

## Inputs

Start and end date, exchange 1 with optional currency, and exchange 2 with optional currency

## Outputs

A CSV containing the following columns:

* time in Unix format

* time in human readable format

* Spread

* Exchange A price converted to USD

* Exchange B price converted to USD

* Original exchange A price

* USD exchange at that time for that currency

* Original exchange B price

* USD exchange rate at that time for that currency

## Uses

Uses module `spread_calc` which exposes a function:

`function convertSpread({exchAPrice,exchAUSDRate,exchBPrice,exchBUSDRate})`

and returns the spread as percentage.  exchXPrice and exchXUSDRate represent
for example bitflyJPY price and the USD to JPY exchange rate at that time.

