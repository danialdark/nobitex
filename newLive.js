const axios = require('axios');
const pgp = require('pg-promise')(); // Import and configure pg-promise
const moment = require('moment');

const db = require('./db'); // Adjust the path as needed
const Redis = require('ioredis');

const redis = new Redis({
    host: '87.107.190.181',
    port: '6379',
    password: 'D@n!@l12098',
    enableCompression: true,
});

var pipeline = redis.pipeline();

let requestCounter = 0;

const tableMap = {
    "1M": "one_month_nobitex_candles",
    "1w": "one_week_nobitex_candles",
    "1d": "one_day_nobitex_candles",
    "4h": "four_hour_nobitex_candles",
    "1h": "one_hour_nobitex_candles",
    "30m": "thirty_minute_nobitex_candles",
    "15m": "fifteen_minute_nobitex_candles",
    "5m": "five_minute_nobitex_candles",
    "1m": "one_minute_nobitex_candles",
    "1s": "one_second_nobitex_candles",
};
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




const fetchCandlestickData = async (symbolName, timeFrame, currentTimestampInSeconds) => {
    try {
        const response = await axios.get(`https://api.nobitex.net/market/udf/history?symbol=${symbolName}&resolution=${timeFrame}&from=0&to=${currentTimestampInSeconds}&countback=2`, { timeout: 15000 });
        return response.data;
    } catch (error) {
        console.log(error)
        sleep(20000)
        console.error(`Received a error. Restarting the app...`);
        process.exit(1)
    }
};

const processCandlestickData = (symbolId, symbolName, data) => {
    if (data.s === "no_data") {
        return [];
    }

    if (data.t.length === 1) {
        const formattedDateTime = moment(data.t[0] * 1000).utcOffset(0).format('YYYY-MM-DD HH:mm:ss');
        return [{
            symbol_id: symbolId,
            symbol_name: symbolName,
            open_time: data.t[0] * 1000,
            open_price: (data.o[0]),
            high_price: (data.h[0]),
            low_price: (data.l[0]),
            close_price: (data.c[0]),
            volumn: data.v[0],
            close_time: 0, // Assuming close_time is the next timestamp
            created_at: formattedDateTime,
        }];
    }

    return data.t.map((timestamp, index) => {
        const formattedDateTime = moment(timestamp * 1000).utcOffset(0).format('YYYY-MM-DD HH:mm:ss');
        return {
            symbol_id: symbolId,
            symbol_name: symbolName,
            open_time: timestamp * 1000,
            open_price: (data.o[index]),
            high_price: (data.h[index]),
            low_price: (data.l[index]),
            close_price: (data.c[index]),
            volumn: data.v[index],
            close_time: 0, // Assuming close_time is the next timestamp
            created_at: formattedDateTime,
        };
    });
};

const shouldMakeAllTimeFrames = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M'];

function getFirstDayOfMonthNotSaturday() {
    const currentDate = new Date();
    const firstDay = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1);

    while (firstDay.getDay() === 6) {
        // If the first day is Saturday (getDay() returns 6 for Saturday),
        // increment the day until a non-Saturday is found
        firstDay.setDate(firstDay.getDate() + 1);
    }

    return firstDay;
}

// isHalf be in mani hast ke aya namad rooye 1 baz mishe ya 1 o 30 dar time frame haye 30 1 4 1d
const checkConfigTime = async (candleTimeStamp, symbolConfig, timeFrame, oneMinuteTime) => {
    const oneMinuteCandleTime = new Date(oneMinuteTime+12600);
    const dayOfWeek = oneMinuteCandleTime.getUTCDay(); //0 is sunday
    const dayOfMonth = oneMinuteCandleTime.getUTCDate();  //0 is sunday
    const candleHour = oneMinuteCandleTime.getUTCHours();
    const candleMinute = oneMinuteCandleTime.getUTCMinutes();

    const myCandleTime = new Date(candleTimeStamp+12600);
    const myCandleHour = myCandleTime.getUTCHours();
    const myCandleMinute = myCandleTime.getUTCMinutes();
    const dayOfCandle = myCandleTime.getUTCDate();


    if (timeFrame == "5m" || timeFrame == "15m" || timeFrame == "30m") {
        const filteredArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num > candleMinute);
        const AllArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num >= 0);

        const biggerTime = Math.min(...filteredArray);

        // Find the index of the smallest Number
        const minIndex = AllArray.indexOf(biggerTime);

        const oneBeforBigger = minIndex != 0 ? AllArray[minIndex - 1] : AllArray[0];
        // yani hanooz be candle badi nareside va bayad edame bede
        if (oneBeforBigger <= myCandleMinute && myCandleMinute < biggerTime) {
            return true;
        } else {
            return false;

        }


    } else {
        const filteredArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num > candleHour);
        var AllArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num >= 0);


        var biggerTime = Math.min(...filteredArray);



        // Find the index of the smallest Number
        const minIndex = AllArray.indexOf(biggerTime);

        var oneBeforBigger = minIndex != 0 ? AllArray[minIndex - 1] : AllArray[0];

        if (biggerTime == undefined || biggerTime == Infinity || biggerTime == AllArray[0]) {


            biggerTime = AllArray[AllArray.length - 1];
            oneBeforBigger = AllArray[0];



            // inja miaym shart haye estesnaye rooz o maho hafte ro mizarim 
            if (timeFrame == "1d") {
                if ((candleHour >= biggerTime) && (dayOfMonth != dayOfCandle)) {
                    // yani candle jadid bayad baz beshe
                    return false;
                } else {
                    return true;
                }
            }

            // inja miaym shart haye estesnaye rooz o maho hafte ro mizarim 
            if (timeFrame == "1w") {
                if ((candleHour >= biggerTime) && (dayOfMonth != dayOfCandle) && dayOfWeek == 0) {
                    return false;
                } else {
                    return true;
                }
            }

            if (timeFrame == "1M") {
                const thisMonth = getFirstDayOfMonthNotSaturday().getDate();

                if ((candleHour >= biggerTime) && (dayOfMonth != dayOfCandle) && dayOfMonth == thisMonth) {
                    return false;
                } else {
                    // yani candle jadid bayad baz beshe
                    return true;
                }
            }

            // this will work for 4h 
            if (biggerTime <= myCandleHour && myCandleHour >= oneBeforBigger) {
                // sat 22 23
                return true;
            } else {
                // for 0 1 
                if (myCandleHour <= oneBeforBigger) {
                    return true
                } else {
                    return false;

                }


            }
        }



        // yani hanooz be candle badi nareside va bayad edame bede
        if (oneBeforBigger <= myCandleHour && myCandleHour < biggerTime) {
            return true;
        } else {
            // yani candle jadid bayad baz beshe
            return false;
        }

    }
}

async function makeMyOpenTime(symbolConfig, timeFrame) {
    const candleTime = new Date();
    var dayOfMonth = candleTime.getUTCDate();
    const candleHour = candleTime.getUTCHours();
    const candleMinute = candleTime.getUTCMinutes();
    const candleYear = candleTime.getUTCFullYear();
    const candleMonth = candleTime.getUTCMonth();
    const dayOfWeek = candleTime.getUTCDay(); //0 is sunday

    if (timeFrame == "5m" || timeFrame == "15m" || timeFrame == "30m") {

        const AllArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num >= 0);
        const filteredArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num > candleMinute);

        // Remove numbers less than candleMinute

        const biggerTime = Math.min(...filteredArray);

        // Find the index of the smallest Number
        const minIndex = AllArray.indexOf(biggerTime);

        const oneBeforBigger = minIndex != 0 ? AllArray[minIndex - 1] : AllArray[0];

        // yani hanooz be candle badi nareside va bayad edame bede
        if (oneBeforBigger < candleMinute < biggerTime) {

            return new Date(Date.UTC(candleYear, candleMonth, dayOfMonth, candleHour, oneBeforBigger)).getTime() / 1000;
        } else {
            // yani candle jadid bayad baz beshe

            return new Date(Date.UTC(candleYear, candleMonth, dayOfMonth, candleHour, biggerTime)).getTime() / 1000;

        }


    } else {
        const AllArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num >= 0);
        const filteredArray = symbolConfig[dayOfWeek][timeFrame].filter(num => num > candleHour);

        const shouldAdd = 30;
        // Remove numbers less than candleHour

        const shouldRemoveHour = symbolConfig.isHalf ? 1 : 0;
        // Remove numbers less than candleHour

        if (timeFrame == "1w") {
            const today = new Date();
            const currentDay = today.getDay();
            const daysToMonday = (currentDay === 0 ? 6 : currentDay - 1); // Calculate days from today to Monday

            const firstDay = new Date(today);
            firstDay.setDate(today.getDate() - daysToMonday); // Set to the first day of the week (Monday)

            dayOfMonth = firstDay.getUTCDate();
        }

        if (timeFrame == "1M") {
            dayOfMonth = 1;
        }

        var biggerTime = Math.min(...filteredArray);
        if (biggerTime == undefined || biggerTime == Infinity || biggerTime == AllArray[0]) {
            return new Date(Date.UTC(candleYear, candleMonth, dayOfMonth, AllArray[AllArray.length - 1] - shouldRemoveHour, 0 + shouldAdd)).getTime() / 1000;
        }
        // Find the index of the biggerTime Number
        const minIndex = AllArray.indexOf(biggerTime);

        const oneBeforBigger = (minIndex != 0 && minIndex != Infinity) ? AllArray[minIndex - 1] : AllArray[0];


        // yani hanooz be candle badi nareside va bayad edame bede
        if (oneBeforBigger < candleHour < biggerTime) {
            return new Date(Date.UTC(candleYear, candleMonth, dayOfMonth, oneBeforBigger - shouldRemoveHour, 0 + shouldAdd)).getTime() / 1000;
        } else {
            // yani candle jadid bayad baz beshe
            return new Date(Date.UTC(candleYear, candleMonth, dayOfMonth, biggerTime - shouldRemoveHour, 0 + shouldAdd)).getTime() / 1000;

        }

    }

}

const candleChecker = async (timeFrame, allCandles, symbolConfig, candleStamp) => {
    // aval check mikonim candle az ghabl vojood darad ya na
    if (allCandles[timeFrame][0] != undefined) {
        // check mishavad ke aya bayad edame dade shavad ya kheir
        // bayad check konim ke data ke alan oomade az lahaze zamani ba config set hast ya na?
        const checker = await checkConfigTime(allCandles[timeFrame][0].t, symbolConfig, timeFrame, candleStamp)
        // console.log(checker +" for " + timeFrame)
        return checker;
    }

    // agar vojood nadarad barash yeki baz mikonim dar zamane moshakhas
    else {
        return false;
    }

}


async function warmUp(symbolName, allCandles) {
    const timeFrames = ['D', '240', '60', '30', '15', '5', '1'];
    const rechangeTimes = {
        'D': "1d",
        '240': "4h",
        '60': "1h",
        '30': "30m",
        '15': "15m",
        '5': "5m",
        '1': "1m"
    }
    const fetchedSymbolId = await getSymbolIdByName(symbolName);

    for (const timeFrame of timeFrames) {
        const currentTimestampInSeconds = Math.floor(Date.now() / 1000);

        try {

            const candlestickData = await fetchCandlestickData(symbolName, timeFrame, currentTimestampInSeconds);
            const processedData = await processCandlestickData(fetchedSymbolId, symbolName, candlestickData);
            if (processedData.length >= 2) {
                const lastTwoCandlesticks = processedData.slice(-2);
                const newCandle = {
                    t: lastTwoCandlesticks[1].open_time,
                    T: 0,
                    o: formatNumberWithTwoDecimals(lastTwoCandlesticks[1].open_price),
                    h: formatNumberWithTwoDecimals(lastTwoCandlesticks[1].high_price),
                    l: formatNumberWithTwoDecimals(lastTwoCandlesticks[1].low_price),
                    c: formatNumberWithTwoDecimals(lastTwoCandlesticks[1].close_price),
                    v: lastTwoCandlesticks[1].volumn != undefined ? lastTwoCandlesticks[1].volumn : 0,
                };

                allCandles[rechangeTimes[timeFrame]][0] = newCandle;

                const newCandle2 = {
                    t: lastTwoCandlesticks[0].open_time,
                    T: 0,
                    o: formatNumberWithTwoDecimals(lastTwoCandlesticks[0].open_price),
                    h: formatNumberWithTwoDecimals(lastTwoCandlesticks[0].high_price),
                    l: formatNumberWithTwoDecimals(lastTwoCandlesticks[0].low_price),
                    c: formatNumberWithTwoDecimals(lastTwoCandlesticks[0].close_price),
                    v: lastTwoCandlesticks[0].volumn != undefined ? lastTwoCandlesticks[0].volumn : 0,
                };

                allCandles[rechangeTimes[timeFrame]][1] = newCandle2;

            }

        } catch (error) {
            console.log(error)
            sleep(20000)
            console.error(`Received a error. Restarting the app...`);
            process.exit(1)

        }
    }

}


const makeOtherCandles = async (allCandles, smallestTimeFrame, lastVolume, symbolName, lastTimeStamp) => {

    // now we will make other candles from 1 minute last candle
    const indexToKeep = shouldMakeAllTimeFrames.indexOf(smallestTimeFrame);
    var resultArray = null

    if (indexToKeep !== -1) {
        resultArray = shouldMakeAllTimeFrames.slice(indexToKeep + 1);
    } else {
        console.log(`The element ${smallestTimeFrame} was not found in the array.`);
    }


    const lastOneMinuteCandle = allCandles[smallestTimeFrame][0];
    const candleStamp = allCandles[smallestTimeFrame][0].t;



    const symbolConfig = {
        0: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        1: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        2: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        3: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        4: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        5: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        6: {
            "5m": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
            "15m": [0, 15, 30, 45, 60],
            "30m": [0, 30, 60],
            "1h": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
            "4h": [0, 4, 8, 12, 16, 20, 24],
            "1d": [0, 24],
            "1w": [0, 24],
            "1M": [0, 24],
        },
        isHalf: false
    }

    if (lastOneMinuteCandle != undefined) {
        for (const timeframe of resultArray) {
            var shouldContinueCandle = false;
            var addedTime = 0;
            var startTime = 0;
            var newV = false
            var checker = await candleChecker(timeframe, allCandles, symbolConfig, candleStamp);
            switch (timeframe) {
                case '5m':
                    addedTime = 300;
                    break;

                case '15m':
                    addedTime = 1500;
                    break;

                case '30m':
                    addedTime = 3000;
                    break;

                case '1h':
                    addedTime = 6000;
                    break;

                case '4h':
                    addedTime = 24000;
                    break;

                case '1d':
                    addedTime = 86400;
                    break;

                case '1w':
                    addedTime = 604800;
                    break;

                case '1M':
                    addedTime = 2629743;
                    break;

                default:
                    addedTime = 0;
                    break;
            }


            if (checker) {
                shouldContinueCandle = true;
                startTime = allCandles[timeframe][0].t;
                timestamp = startTime; // Unix timestamp in seconds
            } else {
                const madeOpenTime = await makeMyOpenTime(symbolConfig, timeframe);
                startTime = madeOpenTime * 1000
            }

            var shouldBe = 0;
            var openPrice, high, low, closeTime;


            // this is for v
            if (shouldContinueCandle) {


                if (+lastOneMinuteCandle.t == +lastTimeStamp) {
                    shouldBe = +lastOneMinuteCandle.v >= +lastVolume
                        ? allCandles[timeframe][0].v + lastOneMinuteCandle.v - lastVolume
                        : allCandles[timeframe][0].v + lastOneMinuteCandle.v;
                } else {
                    shouldBe = allCandles[timeframe][0].v + lastOneMinuteCandle.v;
                }

                openPrice = allCandles[timeframe][0].o;
                closeTime = allCandles[timeframe][0].t + addedTime


                if (allCandles[timeframe][0].h < lastOneMinuteCandle.h) {
                    high = lastOneMinuteCandle.h
                } else {
                    high = allCandles[timeframe][0].h
                }

                if (allCandles[timeframe][0].l > lastOneMinuteCandle.l) {
                    low = lastOneMinuteCandle.l
                } else {
                    low = allCandles[timeframe][0].l
                }
            } else {
                // console.log("####################################")
                shouldBe = lastOneMinuteCandle.v;
                openPrice = lastOneMinuteCandle.o;
                high = lastOneMinuteCandle.h
                low = lastOneMinuteCandle.l
                closeTime = lastOneMinuteCandle.t + addedTime
            }




            const newCandle = {
                t: startTime,
                T: closeTime,
                o: openPrice,
                h: high,
                l: low,
                c: lastOneMinuteCandle.c,
                v: shouldBe,
            };


            // now we will add to each time Frame
            const existingCandleIndex = allCandles[timeframe].findIndex((candle) => candle.t == newCandle.t);

            if (existingCandleIndex >= 0) {
                // Update existing candle
                allCandles[timeframe][existingCandleIndex] = newCandle;
            } else {

                // Add new candle at the beginning
                await allCandles[timeframe].unshift(newCandle);

                if (allCandles[timeframe].length >= 2) {

                    if (allCandles[timeframe][1] != undefined) {

                        const shouldSaveCandleFirst = {
                            t: allCandles[timeframe][0].t,
                            T: allCandles[timeframe][0].T,
                            c: allCandles[timeframe][0].c,
                            h: allCandles[timeframe][0].h,
                            l: allCandles[timeframe][0].l,
                            o: allCandles[timeframe][0].o,
                            v: allCandles[timeframe][0].v,
                        };
                        // console.log(shouldSaveCandleFirst)
                        saveCandleDataToPostgreSQL(symbolName, timeframe, shouldSaveCandleFirst);

                        const shouldSaveCandle = {
                            t: allCandles[timeframe][1].t,
                            T: allCandles[timeframe][1].T,
                            c: allCandles[timeframe][1].c,
                            h: allCandles[timeframe][1].h,
                            l: allCandles[timeframe][1].l,
                            o: allCandles[timeframe][1].o,
                            v: allCandles[timeframe][1].v,
                        };

                        saveCandleDataToPostgreSQL(symbolName, timeframe, shouldSaveCandle);

                        if (allCandles[timeframe].length == 3) {
                            // Remove excess candles
                            await allCandles[timeframe].pop();
                        }
                    }
                }


            }
        }
        redis.pipeline().set(`${symbolName.toLowerCase()}`, JSON.stringify(allCandles)).expire(`${symbolName.toLowerCase()}`, 259200).exec();

    }
}

const startnobitexHistory = async (symbol, symbols, allCandles) => {
    const symbolName = symbol.toUpperCase();
    const fetchedSymbolId = await getSymbolIdByName(symbolName);
    var lastVolume = 0;
    var lastTimeStamp = 0;
    var shouldUpdate = false;
    while (true) {

        try {
            if (requestCounter >= symbols.length) {
                await sleep(1 * 1000); // Sleep for 1 minute
            }
            const currentTimestampInSeconds = Math.floor(Date.now() / 1000);

            requestCounter++
            const candlestickData = await fetchCandlestickData(symbolName, "1", currentTimestampInSeconds);
            const processedData = await processCandlestickData(fetchedSymbolId, symbolName, candlestickData);
            const reversedData = processedData.reverse();
            if (reversedData[0] == undefined) {
                continue;
            }

            if (reversedData.length == 1) {
                shouldUpdate = true;
            }

            let newCandle = null;
            let oldCandle = null;
            newCandle = {
                t: reversedData[0].open_time,
                T: reversedData[0].open_time + 60000,
                o: formatNumberWithTwoDecimals(reversedData[0].open_price),
                h: formatNumberWithTwoDecimals(reversedData[0].high_price),
                l: formatNumberWithTwoDecimals(reversedData[0].low_price),
                c: formatNumberWithTwoDecimals(reversedData[0].close_price),
                v: reversedData[0].volumn,
            };

            // this will update last candle
            if (reversedData[1] != undefined) {
                oldCandle = {
                    t: processedData[1].open_time,
                    T: processedData[1].open_time + 60000,
                    o: formatNumberWithTwoDecimals(processedData[1].open_price),
                    h: formatNumberWithTwoDecimals(processedData[1].high_price),
                    l: formatNumberWithTwoDecimals(processedData[1].low_price),
                    c: formatNumberWithTwoDecimals(processedData[1].close_price),
                    v: processedData[1].volumn,
                };
            }


            if (reversedData.length == 2 && shouldUpdate) {
                newCandle = oldCandle;
                shouldUpdate = false
            }



            if (allCandles['1m'][0] != undefined) {
                lastVolume = allCandles['1m'][0].v;
                lastTimeStamp = allCandles['1m'][0].t;
            }

            if (oldCandle != null) {
                allCandles['1m'][1] = oldCandle;
            }

            const existingCandleIndex = allCandles['1m'].findIndex((candle) => candle.t == newCandle.t);
            if (existingCandleIndex >= 0) {
                // Update existing candle
                allCandles['1m'][existingCandleIndex] = newCandle;
            } else {
                // Add new candle at the beginning
                allCandles['1m'].unshift(newCandle);

                if (allCandles['1m'].length >= 2) {

                    if (allCandles["1m"][1] != undefined) {
                        const shouldSaveCandleFirst = {
                            t: allCandles["1m"][0].t,
                            T: allCandles["1m"][0].T,
                            c: allCandles["1m"][0].c,
                            h: allCandles["1m"][0].h,
                            l: allCandles["1m"][0].l,
                            o: allCandles["1m"][0].o,
                            v: allCandles["1m"][0].v,
                        };

                        saveCandleDataToPostgreSQL(symbolName, "1m", shouldSaveCandleFirst);

                        const shouldSaveCandle = {
                            t: allCandles["1m"][1].t,
                            T: allCandles["1m"][1].T,
                            c: allCandles["1m"][1].c,
                            h: allCandles["1m"][1].h,
                            l: allCandles["1m"][1].l,
                            o: allCandles["1m"][1].o,
                            v: allCandles["1m"][1].v,
                        };

                        saveCandleDataToPostgreSQL(symbolName, "1m", shouldSaveCandle);



                        if (allCandles["1m"].length == 3) {
                            // Remove excess candles
                            allCandles["1m"].pop();
                        }
                    }
                }
            }

            await makeOtherCandles(allCandles, "1m", lastVolume, symbolName, lastTimeStamp)
            // console.log(allCandles)
            redis.pipeline().set(`${symbolName.toLowerCase()}`, JSON.stringify(allCandles)).expire(`${symbolName.toLowerCase()}`, 259200).exec();
        } catch (error) {
            console.log(error)
            sleep(20000)
            console.error(`Received a error. Restarting the app...`);
            process.exit(1)

        }
    }
};


async function saveCandleDataToPostgreSQL(symbol, timeFrame, newCandle) {
    const fetchedSymbolId = await getSymbolIdByName(symbol.toUpperCase());
    const timestampMilliseconds = newCandle.t; // Unix timestamp in milliseconds
    const modifiedFormattedDateTime = moment(timestampMilliseconds).utc().format('YYYY-MM-DD HH:mm:ss');
    try {
        await db.none(
            `INSERT INTO ${tableMap[timeFrame]} (symbol_id, symbol_name, open_time, open_price, high_price, low_price, close_price, volumn, close_time, created_at) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol_name, created_at) DO UPDATE
            SET 
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volumn = excluded.volumn,
                close_time = excluded.close_time,
                created_at = excluded.created_at`,
            [
                fetchedSymbolId,
                symbol.toUpperCase(),
                newCandle.t,
                newCandle.o,
                newCandle.h,
                newCandle.l,
                newCandle.c,
                newCandle.v != null ? newCandle.v : 0,
                newCandle.T,
                modifiedFormattedDateTime,
            ]
        );

        // console.log(`data saved to ${timeFrame} for ${symbol}`)
    } catch (error) {
        console.error('Error saving candle data to PostgreSQL:', error);
    }
}


const getLive = async (symbols) => {
    const chunkSize = 150;
    const symbolChunks = [];



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
                var allCandles = { "1m": [], "5m": [], "15m": [], "30m": [], "1h": [], "4h": [], "1d": [], "1w": [], "1M": [] };

                // await warmUp(symbol.toLowerCase(), allCandles)
                startnobitexHistory(symbol.toLowerCase(), symbols, allCandles);
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

                console.log(`***activating live  for ${symbol}***`);
            });
        }
        // processNextChunk();
    };

    processNextChunk();
};



module.exports = { getLive, updateRedisPrice }