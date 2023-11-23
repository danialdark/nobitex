const db = require('./db'); // Adjust the path as needed
const axios = require('axios');

async function fetchSymbolsFromAPI() {
    try {
        const response = await axios.get("https://api.nobitex.ir/v2/orderbook/all");
        return response.data;
    } catch (error) {
        console.error('Error fetching symbols from API:', error);
        return null;
    }
}

async function insertOrUpdateSymbolToDatabase(symbol, pricescale, formattedDateTime) {
    try {
        await db.none(
            `INSERT INTO nobitex_symbols (name, quote_precision, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (name) DO UPDATE
            SET
                name = excluded.name,
               
                quote_precision = excluded.quote_precision,
                created_at = excluded.created_at`,
            [
                symbol.toUpperCase(),
                pricescale,
                formattedDateTime,
            ]
        );
        console.log(`Inserted/Updated symbol: ${symbol}`);
    } catch (error) {
        console.error('Error saving symbol to PostgreSQL:', error);
    }
}





async function processSymbols() {
    const symbolsData = await fetchSymbolsFromAPI();
    const IRTSYMBOLS = [];
    if (!symbolsData) {
        console.error('Unable to fetch symbols data from API. Exiting.');
        return;
    }

    const currentTimestamp = new Date().getTime(); // Unix timestamp in milliseconds
    const formattedDateTime = new Date(currentTimestamp).toISOString().slice(0, 19).replace('T', ' ');

    const symbols = Object.keys(symbolsData);

    for (const symbol of symbols) {
        if (symbol.endsWith("IRT")) {
            const { description, lastTradePrice } = symbolsData[symbol];
            IRTSYMBOLS.push(symbol)
            await insertOrUpdateSymbolToDatabase(symbol, 1, formattedDateTime);

        }
    }

    return IRTSYMBOLS;

}

// Call the function to start the process
module.exports = processSymbols;