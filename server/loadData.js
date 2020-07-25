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

  for (let i = 0; i < files.length; ++i) {
    const f = files[i]

    const readFileInterface = readline.createInterface({
      input: fs.createReadStream(f),
      crlfDelay: Infinity
    })

    const fileContents = fs.readFileSync(f, 'utf-8')
    let x = 0
    for await (const r of readFileInterface) {
      if (x++ === 0) continue
      data.push(splitRowIntoFields(r))
    }

    if (data.length > 50000) {
      const beginTime = process.hrtime()
      cl(`About to INSERT ${data.length} records`)

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
      cl(`Finished INSERT after ${finishTime} ms`)
    }
  }
}

async function readFiles(folderPath) {
  const pathNASDAQ = `${folderPath}/NASDAQ`
  const pathNYSE = `${folderPath}/NYSE`

  const files = []
  files.push(...fs.readdirSync(pathNASDAQ).map(f => `${pathNASDAQ}/${f}`))
  files.push(...fs.readdirSync(pathNYSE).map(f => `${pathNYSE}/${f}`))

  await sqlQuery(fs.readFileSync(`./sqlFiles/createOHLC.sql`, 'utf-8'))

  return files
}

async function init() {
  const files = await readFiles(folderPath)
  await insertIntoDb(files)
  console.log('DONE')
}

init().then(() => { cl(`Finished execution`); process.exit()})