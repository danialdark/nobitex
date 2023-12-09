const axios = require('axios');
const pgp = require('pg-promise')(); // Import and configure pg-promise
const moment = require('moment');

const db = require('./db'); // Adjust the path as needed
const Redis = require('ioredis');

const redis = new Redis({
    host: 'localhost',
    port: '6379',
    password: 'D@n!@l12098',
    enableCompression: true,
});

var pipeline = redis.pipeline();


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


async function updateRedisPrice(symbols) {
    try {
        const response = await axios.get(`https://api.nobitex.net/v2/orderbook/all`);

        for (const symbol of symbols) {
            const symbolKey = symbol.toLowerCase();
            const existingData = await redis.get(symbolKey);
            const data = JSON.parse(existingData);

            for (const timeFrame in data) {
                if (data.hasOwnProperty(timeFrame)) {
                    try {
                        var newPrice = response.data[symbol.toUpperCase()].lastTradePrice;

                        if (newPrice !== undefined) {
                            data[timeFrame][0].c = (newPrice / 10) + ".00";
                            // console.log(newPrice / 10 + ".00")
                        }
                    } catch (priceError) {
                        sleep(20000)
                        console.error(`Error updating price for ${symbol}: ${priceError.message}`);
                        process.exit(1)
                    }
                }
            }

            await redis.set(symbolKey, JSON.stringify(data));
        }
    } catch (error) {
        sleep(20000)
        console.error(`Error fetching data from API: ${error.message}`);
        process.exit(1)
    }
}


// Helper function to get the time frame key
const getTimeFrameKey = (timeFrame) => {
    switch (timeFrame) {
        case "1":
            return "1m";
        case "5":
            return "5m";
        case "15":
            return "15m";
        case "30":
            return "30m";
        case "60":
            return "1h";
        case "240":
            return "4h";
        case "D":
            return "1d";
        case "1w":
            return "1w";
        default:
            console.log(timeFrame)

            return null;
    }
};


const saveCandlesToRedis = async (symbol, timeFrame, batch) => {
    const symbolKey = symbol.toLowerCase();
    const existingData = await redis.get(symbolKey);
    let dataToUpdate = {};
    if (existingData != "null" && existingData != null) {
        try {
            dataToUpdate = JSON.parse(existingData);
        } catch (error) {
            console.error('Error parsing existing data from Redis:', error);
        }
    } else {
        dataToUpdate = {};
    }


    const timeFrameKey = getTimeFrameKey(timeFrame); // Helper function to get the time frame key

    if (!dataToUpdate[timeFrameKey]) {
        dataToUpdate[timeFrameKey] = [];
    }

    const candlestickData = batch.slice(-2).map(candle => ({
        t: candle.open_time,
        T: candle.close_time,
        c: candle.close_price,
        h: candle.high_price,
        l: candle.low_price,
        o: candle.open_price,
        v: candle.volumn
    })).reverse();

    dataToUpdate[timeFrameKey] = [...dataToUpdate[timeFrameKey], ...candlestickData];

    // Only keep the last two candles
    if (dataToUpdate[timeFrameKey].length > 2) {
        dataToUpdate[timeFrameKey] = dataToUpdate[timeFrameKey].slice(-2);
    }

    await redis.set(symbolKey, JSON.stringify(dataToUpdate));
    // console.log(`saved for ${timeFrame}`)

};



const fetchCandlestickData = async (symbolName, timeFrame, currentTimestampInSeconds) => {
    try {
        const response = await axios.get(`https://api.nobitex.net/market/udf/history?symbol=${symbolName}&resolution=${timeFrame}&from=0&to=${currentTimestampInSeconds}`, { timeout: 15000 });
        return response.data;
    } catch (error) {
        console.log(error)
        sleep(20000)
        console.error(`Received a error. Restarting the app...`);
        process.exit(1)
    }
};

const processCandlestickData = (symbolId, symbolName, data) => {
    if (data.s === "no_data" || data.t.length === 1) {
        return [];
    }

    return data.t.map((timestamp, index) => {
        const formattedDateTime = moment(timestamp * 1000).utcOffset(0).format('YYYY-MM-DD HH:mm:ss');
        return {
            symbol_id: symbolId,
            symbol_name: symbolName,
            open_time: timestamp * 1000,
            open_price: formatNumberWithTwoDecimals(data.o[index]),
            high_price: formatNumberWithTwoDecimals(data.h[index]),
            low_price: formatNumberWithTwoDecimals(data.l[index]),
            close_price: formatNumberWithTwoDecimals(data.c[index]),
            volumn: data.v[index],
            close_time: 0, // Assuming close_time is the next timestamp
            created_at: formattedDateTime,
        };
    });
};

const startnobitexHistory = async (symbol) => {
    const timeFrames = ['D', '240', '60', '30', '15', '5', '1'];
    const symbolName = symbol.toUpperCase();
    const fetchedSymbolId = await getSymbolIdByName(symbolName);
    const currentTimestampInSeconds = Math.floor(Date.now() / 1000);

    for (const timeFrame of timeFrames) {
        let flag = true;
        try {
            while (flag) {
                if (requestCounter >= 700) {
                    requestCounter = 0
                    await sleep(30 * 1000); // Sleep for 1 minute
                }
                const candlestickData = await fetchCandlestickData(symbolName, timeFrame, currentTimestampInSeconds);
                requestCounter++;
                // console.log(`request sent for ${symbolName} for ${timeFrame}`)
                if (candlestickData.length === 0) {
                    flag = false;
                    continue;
                }

                const processedData = processCandlestickData(fetchedSymbolId, symbolName, candlestickData);

                if (processedData.length >= 2) {
                    const lastTwoCandlesticks = processedData.slice(-2);
                    await saveCandlesToRedis(symbol, timeFrame, lastTwoCandlesticks);
                    await insertCandlestickBatch(getTableName(timeFrame), lastTwoCandlesticks);
                }

                flag = false;

            }
        } catch (error) {
            console.log(error)
            sleep(20000)
            console.error(`Received a error. Restarting the app...`);
            process.exit(1)

        }
    }

    return true;
};

// Helper function to get the table name
const getTableName = (timeFrame) => {
    switch (timeFrame) {
        case "1M":
            return "one_month_nobitex_candles";
        case "1w":
            return "one_week_nobitex_candles";
        case "D":
            return "one_day_nobitex_candles";
        case "240":
            return "four_hour_nobitex_candles";
        case "60":
            return "one_hour_nobitex_candles";
        case "30":
            return "thirty_minute_nobitex_candles";
        case "15":
            return "fifteen_minute_nobitex_candles";
        case "5":
            return "five_minute_nobitex_candles";
        case "1":
            return "one_minute_nobitex_candles";
        case "1s":
            return "one_second_nobitex_candles";
        default:
            return null;
    }
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
                open_price: record.open_price,
                high_price: record.high_price,
                low_price: record.low_price,
                close_price: record.close_price,
                volumn: record.volumn,
                close_time: record.close_time,
                created_at: record.created_at
            }));

            const query = pgp.helpers.insert(values, cs) +
                ` ON CONFLICT (symbol_name, created_at)
                DO UPDATE
                SET 
                    open_time = EXCLUDED.open_time,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volumn = EXCLUDED.volumn,
                    close_time = EXCLUDED.close_time`;

            await t.none(query);

            // console.log(`Data inserted or updated into ${tableName} for ${batch.length} records`);
        });
    } catch (error) {
        console.error('Error:', error.message);
    }
};


const getLive = async (symbols) => {
    const chunkSize = 10;
    const symbolChunks = [];
    const delayTime = 3000;



    let currentIndex = 0;
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
                return startnobitexHistory(symbol.toLowerCase());
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

                // console.log(`***done getting history for ${symbol}***`);
            });
        } else {

            // Rerun the function if needed
            getLive(symbols);

            return;
        }
        processNextChunk();
    };

    processNextChunk();
};



module.exports = { getLive, updateRedisPrice }