const chalk = require('chalk')
const fs = require('fs')
const readline = require('readline')
const { sqlQuery } = require('./postgres')
const folderPath = './data'
const cl = console.log

function splitRowIntoFields(row) {
  const r = row.split(`,`)
  const s = {}

  try {
    if (r.length !== 7) {
      s.e = `Invalid record`
    } else {
      s.ticker = r[0]
      s.date = new Date(`${r[1].slice(0,4)}-${r[1].slice(4,6)}-${r[1].slice(6,8)}T09:30:00`)
        .toISOString()
      s.open = parseFloat(r[2])
      s.high = parseFloat(r[3])
      s.low = parseFloat(r[4])
      s.close = parseFloat(r[5])
      s.vol = parseInt(r[6])
    }
  } catch (e) {
    s.e = e.message ? e.message : `Exception occured`
  } finally {
    if (s.e) {
      cl(`Error in ${row} => ${s.e}`)
      process.exit()
    } else return s
  }
}

async function insertIntoDb(files) {
  const data = []
  let lastbeginTime = process.hrtime()

  for (let i = 0; i < files.length; ++i) {
    const f = files[i]

    const readFileInterface = readline.createInterface({
      input: fs.createReadStream(f),
      crlfDelay: Infinity
    })

    const fileContents = fs.readFileSync(f, 'utf-8')
    data.push(...fileContents.split(/\r?\n/).slice(1).filter((r) => {
      if (r.length === 0) return false
      else return true }).map((r) => {
        return splitRowIntoFields(r)
      })
    )

    if (data.length > 50000) {
      const beginTime = process.hrtime()
      const dataLen = data.length

      const r0 = Object.assign({}, data[0])
      const rn = Object.assign({}, data[dataLen - 1])

      let insertQuery = `INSERT INTO "stockData".stockinfo` +
       `("ticker", "date", "open", "high", "low", "close", "volume") VALUES`

      for (let j = 0; j < data.length; ++j) {
        const r = data[j]
        insertQuery += `('${r.ticker}', '${r.date}', '${r.open}', '${r.high}',` +
          ` '${r.low}', '${r.close}', '${r.vol}'),`
      }

      await sqlQuery(insertQuery.slice(0,-1))
      data.length = 0

      const formatProcessTime = (t) => Math.ceil((t[0] * 1e9 + t[1]) / 1e6)
      const finishTime = formatProcessTime(process.hrtime(beginTime))
      const printStr = `INSERTS: ${chalk.green(dataLen)} TOOK ${chalk.green(finishTime)} ms` +
        ` FROM ${chalk.yellow(r0.date.slice(0,10))} [${chalk.red(r0.ticker)}]` +
        ` TO ${chalk.yellow(rn.date.slice(0,10))} [${chalk.red(rn.ticker)}]`
      
      cl(printStr)
      lastBeginTime = process.hrtime()
    }
  }

  let insertQuery = `INSERT INTO "stockData".stockinfo` +
  `("ticker", "date", "open", "high", "low", "close", "volume") VALUES`
  
  for (let j = 0; j < data.length; ++j) {
    const r = data[j]
    insertQuery += `('${r.ticker}', '${r.date}', '${r.open}', '${r.high}',` +
      ` '${r.low}', '${r.close}', '${r.vol}'),`
  }

  const dataLen = data.length
  const r0 = Object.assign({}, data[0])
  const rn = Object.assign({}, data[dataLen - 1])

  await sqlQuery(`${insertQuery.slice(0,-1)} ON CONFLICT DO NOTHING`)
  const formatProcessTime = (t) => Math.ceil((t[0] * 1e9 + t[1]) / 1e6)
  const finishTime = formatProcessTime(process.hrtime(lastBeginTime))
  const printStr = `INSERTS: ${chalk.green(dataLen)} TOOK ${chalk.green(finishTime)} ms` +
    ` FROM ${chalk.yellow(r0.date.slice(0,10))} [${chalk.red(r0.ticker)}]` +
    ` TO ${chalk.yellow(rn.date.slice(0,10))} [${chalk.red(rn.ticker)}]`
  
  cl(printStr)
}

async function readFiles(folderPath) {
  const pathNASDAQ = `${folderPath}/NASDAQ`
  const pathNYSE = `${folderPath}/NYSE`
  const files = []

  fs.readdirSync(pathNASDAQ).forEach(f => {
    fs.readdirSync(`${pathNASDAQ}/${f}`).forEach((file => files.push(`${pathNASDAQ}/${f}/${file}`)))
  })

  fs.readdirSync(pathNYSE).forEach(f => {
    fs.readdirSync(`${pathNYSE}/${f}`).forEach((file => files.push(`${pathNYSE}/${f}/${file}`)))
  })

  await sqlQuery(fs.readFileSync(`./sqlFiles/createOHLC.sql`, 'utf-8'))
  return files
}

async function init() {
  const files = await readFiles(folderPath)
  await insertIntoDb(files)
  console.log('DONE')
}

init().then(() => { cl(`Finished execution`); process.exit()})
