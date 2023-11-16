

const express = require('express');
const app = express();
const port = 3000;

// Middleware: Parse JSON bodies
app.use(express.json());




// for getting symbols name
const getSpotSymbols = require('./symbol.js');

setInterval(getSpotSymbols, 86400000);

// for getting symbols name
const getHistory = require('./history.js');


const live = require('./live.js');

// // this will get all active symbols and start streaming

getSpotSymbols().then((data) => {
    // getHistory(data);
    live(data);
});



app.get('/active', (req, res) => {
    res.send("activated");
});



// Start the server
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});