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

async function insertOrUpdateSymbolToDatabase(symbol, description, pricescale, formattedDateTime) {
    try {
        await db.none(
            `INSERT INTO forex_symbols (name, description, quote_precision, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE
            SET
                name = excluded.name,
                description = excluded.description,
                quote_precision = excluded.quote_precision,
                created_at = excluded.created_at`,
            [
                symbol.toUpperCase(),
                description,
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
            await insertOrUpdateSymbolToDatabase(symbol, description, lastTradePrice, formattedDateTime);

        }
    }

    return IRTSYMBOLS;

}

// Call the function to start the process
module.exports = processSymbols;