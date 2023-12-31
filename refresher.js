const axios = require('axios');
const pgp = require('pg-promise')(); // Import and configure pg-promise
const moment = require('moment');

const db = require('./db'); // Adjust the path as needed


let requestCounter = 0;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


setInterval(() => {
    if (requestCounter > 50) {
        requestCounter - 50;
    }
}, 50000);

async function getSymbolIdByName(symbolName) {
    try {
        const query = 'SELECT id FROM nobitex_symbols WHERE name = $1';
        const symbol = await db.oneOrNone(query, symbolName);
        return symbol ? symbol.id : null;
    } catch (error) {
        console.error('Error:', error.message);
        throw error;
    }
}

function formatNumberWithTwoDecimals(number) {
    let numberStr = number.toString();

    if (numberStr.includes('.')) {
        // Check if there is already a decimal point in the number
        if (numberStr.endsWith('0')) {
            return numberStr + '0';
        } else {
            return numberStr;
        }
    } else {
        // If there's no decimal point, add '.00' to the end
        return numberStr + '.00';
    }
}


const startSpotHistory = async (symbol, timeFrames, maxPage) => {
    const symbolName = symbol.toUpperCase();


    const fetchedSymbolId = await getSymbolIdByName(symbolName)


    for (const timeFrame of timeFrames) {
        let startTime = 0; // Start from the beginning
        let page = 1;
        let currentTimestampInSeconds = Math.floor(Date.now() / 1000);

        // console.log(`start time is ${startTime} for ${timeFrame}`);
        var tableName = null;
        // checking table name
        switch (timeFrame) {
            case "1M":
                tableName = "one_month_nobitex_candles"
                break;
            case "1w":
                tableName = "one_week_nobitex_candles"
                break;
            case "D":
                tableName = "one_day_nobitex_candles"
                break;
            case "240":
                tableName = "four_hour_nobitex_candles"
                break;
            case "60":
                tableName = "one_hour_nobitex_candles"
                break;
            case "30":
                tableName = "thirty_minute_nobitex_candles"
                break;
            case "15":
                tableName = "fifteen_minute_nobitex_candles"
                break;
            case "5":
                tableName = "five_minute_nobitex_candles"
                break;
            case "1":
                tableName = "one_minute_nobitex_candles"
                break;
            case "1s":
                tableName = "one_second_nobitex_candles"
                break;

            default:
                break;
        }

        let flag = true;
        const candlestickBatch = [];
        const usedOpenTimes = []

        while (flag && page <= maxPage) {


            const response = await axios.get(`https://api.nobitex.ir/market/udf/history?symbol=${symbolName}&resolution=${timeFrame}&from=${startTime}&to=${currentTimestampInSeconds}&page=${page}`);
            // requestCounter++;
            page++;
            if (response.status !== 200) {
                throw new Error(`Failed to fetch candlestick data. Status: ${response.status}, Message: ${response.statusText}`);
            }

            if (response.data.s == "no_data" || response.data.t.length == 1) {
                flag = false;
                continue;
            }


            const candlestickData = response.data.t.map((timestamp, index) => {
                const formattedDateTime = moment(timestamp * 1000).utcOffset(0).format('YYYY-MM-DD HH:mm:ss');
                const found = usedOpenTimes.find(usedOpenTime => usedOpenTime == timestamp);
                usedOpenTimes.push(timestamp)
                if (found == undefined) {
                    return {
                        symbol_id: fetchedSymbolId,
                        symbol_name: symbolName,
                        open_time: timestamp * 1000,
                        open_price: formatNumberWithTwoDecimals(response.data.o[index]),
                        high_price: formatNumberWithTwoDecimals(response.data.h[index]),
                        low_price: formatNumberWithTwoDecimals(response.data.l[index]),
                        close_price: formatNumberWithTwoDecimals(response.data.c[index]),
                        volumn: response.data.v[index],
                        close_time: response.data.c[index], // Assuming close_time is the next timestamp
                        created_at: formattedDateTime,
                    };
                }
            });



            if (candlestickData.length === 0 || candlestickData[0] == undefined) {
                flag = false;
                continue;
            }


            candlestickBatch.push(...candlestickData);


            // Check if candlestickBatch reaches 500k and insert it into the database
            if (candlestickBatch.length >= 20000) {
                await insertCandlestickBatch(tableName, candlestickBatch);
                candlestickBatch.length = 0; // Clear the batch after inserting
            }
        }

        // Insert any remaining data in candlestickBatch
        if (candlestickBatch.length > 0) {
            await insertCandlestickBatch(tableName, candlestickBatch);
        }
    }

    return true;
};

const insertCandlestickBatch = async (tableName, batch) => {
    try {
        await db.tx(async (t) => {
            const cs = new pgp.helpers.ColumnSet([
                'symbol_id',
                'symbol_name',
                'open_time',
                'open_price',
                'high_price',
                'low_price',
                'close_price',
                'volumn',
                'close_time',
                'created_at'
            ], { table: tableName });

            const values = batch.map(record => ({
                symbol_id: record.symbol_id,
                symbol_name: record.symbol_name,
                open_time: record.open_time,
                open_price: formatNumberWithTwoDecimals(record.open_price),
                high_price: formatNumberWithTwoDecimals(record.high_price),
                low_price: formatNumberWithTwoDecimals(record.low_price),
                close_price: formatNumberWithTwoDecimals(record.close_price),
                volumn: record.volumn,
                close_time: record.close_time,
                created_at: record.created_at
            }));

            const updateColumns = ['open_price', 'high_price', 'low_price', 'close_price', 'volumn'];

            const query = pgp.helpers.insert(values, cs) +
                ` ON CONFLICT (symbol_name, created_at)
                DO UPDATE SET ${updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ')}`;

            await t.none(query);

            console.log(`Data inserted or updated into ${tableName} for ${batch.length} records`);
        });
    } catch (error) {
        console.error('Error:', error.message);
    }

};


async function removeOldData() {
    const today = new Date().toISOString().split('T')[0];

    await db.any('DELETE FROM one_hour_nobitex_candles WHERE created_at::date >= $1', [today]);
    await db.any('DELETE FROM four_hour_nobitex_candles WHERE created_at::date >= $1', [today]);

    return true;
}


const refresher = async (symbols, timeFrames, consolePage) => {
    await removeOldData();
    const chunkSize = 1;
    const symbolChunks = [];
    // const delayTime = 3000;
    let currentIndex = 0;

    timeFrames = ["1", "5", "15", "30", "60", "240", "D"]
    // Retrieve active symbols from the local PostgreSQL database
    const symbolNames = await db.any('SELECT name FROM nobitex_symbols WHERE status = 1');

    // Extract symbol names from the result
    symbols = symbolNames.map(symbol => symbol.name);



    while (currentIndex < symbols.length) {
        const chunk = symbols.slice(currentIndex, currentIndex + chunkSize);
        symbolChunks.push(chunk);
        currentIndex += chunkSize;
    }

    let currentChunkIndex = 0;
    const getNextChunk = () => {
        if (currentChunkIndex < symbolChunks.length) {
            const symbolsChunk = symbolChunks[currentChunkIndex];
            currentChunkIndex++;

            let counter = 0;
            return symbolsChunk.map(async (symbol) => {
                counter++;
                // await sleep(counter * delayTime); // Wait for 5 seconds
                return startSpotHistory(symbol.toLowerCase(), timeFrames, consolePage);
            });
        } else {
            return null;
        }
    };

    const processNextChunk = async () => {
        const promises = getNextChunk();
        if (promises) {
            const results = await Promise.all(promises);
            results.forEach((result, index) => {
                const symbol = symbolChunks[currentChunkIndex - 1][index];
                console.log(`***done getting history for ${symbol}***`);
            });
        } else {
            console.log('All chunks processed');

            // Rerun the function if needed
            // refresher(symbols, timeFrames, count);

            return;
        }
        processNextChunk();
    };

    processNextChunk();
};

module.exports = refresher 
