const chalk = require('chalk')
const fs = require('fs')
const readline = require('readline')
const { sqlQuery } = require('./postgres')
const cl = console.log

async function init() {
  const data = await sqlQuery(`SELECT * FROM "stockData".stockinfo WHERE ticker = 'AMD' ORDER BY date ASC`)
  const tickerData = data.dbResult.map((r) => { return { k: r.date, v: r.close }})
  fs.writeFileSync(`./amd.json`, JSON.stringify(tickerData))
}

init().then(() => { cl(`Finished execution`); process.exit()})