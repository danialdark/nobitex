const axios = require('axios');
const pgp = require('pg-promise')(); // Import and configure pg-promise
const moment = require('moment');

const db = require('./db'); // Adjust the path as needed
const Redis = require('ioredis');

const redis = new Redis({
    host: '91.107.160.210',
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
        const query = 'SELECT id FROM spot_symbols WHERE name = $1';
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
    const response = await axios.get(`https://api.nobitex.ir/v2/orderbook/all`);

    for (const symbol of symbols) {
        const symbolKey = symbol.toLowerCase();
        const existingData = await redis.get(symbolKey);
        const data = JSON.parse(existingData);
        for (const timeFrame in data) {
            if (data.hasOwnProperty(timeFrame)) {
                var newPrice = response.data[symbol.toUpperCase()].lastTradePrice
                if (newPrice != undefined) {
                    data[timeFrame][0].c = newPrice / 10 + ".00";
                    // console.log(newPrice / 10 + ".00")
                }
            }
        }
        await redis.set(symbolKey, JSON.stringify(data));

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



const startspotHistory = async (symbol) => {
    const timeFrames = ['D', '240', '60', '30', '15', '5', '1'];

    const symbolName = symbol.toUpperCase();

    const fetchedSymbolId = await getSymbolIdByName(symbolName);
    let currentTimestampInSeconds = Math.floor(Date.now() / 1000);

    for (const timeFrame of timeFrames) {
        const tableName = getTableName(timeFrame); // Helper function to get the table name

        const response = await axios.get(`https://api.nobitex.ir/market/udf/history?symbol=${symbolName}&resolution=${timeFrame}&from=0&to=${currentTimestampInSeconds}`);
        if (response.status !== 200) {
            throw new Error(`Failed to fetch candlestick data. Status: ${response.status}, Message: ${response.statusText}`);
        }

        if (response.data.s == "no_data" || response.data.t.length == 1) {
            flag = false;
            continue;
        }

        const candlestickData = response.data.t.map((timestamp, index) => {
            const formattedDateTime = moment(timestamp * 1000).utcOffset(0).format('YYYY-MM-DD HH:mm:ss');

            return {
                symbol_id: fetchedSymbolId,
                symbol_name: symbolName,
                open_time: timestamp * 1000,
                open_price: formatNumberWithTwoDecimals(response.data.o[index]),
                high_price: formatNumberWithTwoDecimals(response.data.h[index]),
                low_price: formatNumberWithTwoDecimals(response.data.l[index]),
                close_price: formatNumberWithTwoDecimals(response.data.c[index]),
                volumn: response.data.v[index],
                close_time: 0, // Assuming close_time is the next timestamp
                created_at: formattedDateTime,
            };
        });

        // Insert the last two candlesticks into the database
        if (candlestickData.length >= 2) {
            const lastTwoCandlesticks = candlestickData.slice(-2);
            await saveCandlesToRedis(symbol, timeFrame, lastTwoCandlesticks);
            // console.log(lastTwoCandlesticks)
            await insertCandlestickBatch(tableName, lastTwoCandlesticks);
        }
    }

    return true;
};

// Helper function to get the table name
const getTableName = (timeFrame) => {
    switch (timeFrame) {
        case "1M":
            return "one_month_spot_candles";
        case "1w":
            return "one_week_spot_candles";
        case "D":
            return "one_day_spot_candles";
        case "240":
            return "four_hour_spot_candles";
        case "60":
            return "one_hour_spot_candles";
        case "30":
            return "thirty_minute_spot_candles";
        case "15":
            return "fifteen_minute_spot_candles";
        case "5":
            return "five_minute_spot_candles";
        case "1":
            return "one_minut_spot_candles";
        case "1s":
            return "one_second_spot_candles";
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
    const chunkSize = 30;
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
                return startspotHistory(symbol.toLowerCase());
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

            // Rerun the function if needed
            getLive(symbols);

            return;
        }
        processNextChunk();
    };

    processNextChunk();
};



module.exports = { getLive, updateRedisPrice }