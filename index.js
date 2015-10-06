//module.exports = require('./lib/influx-connector');

var connector = require('./lib/influx-connector');
module.exports.InfluxDBConnector = connector.InfluxDBConnector;
module.exports.initialize = connector.initialize;
