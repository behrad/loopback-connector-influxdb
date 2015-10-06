'use strict';

var influx = require('influx');
var debug  = require('debug')( 'connector:influxdb' );


exports.initialize = function initializeDataSource(dataSource, callback) {
  var settings = dataSource.settings || {};

  dataSource.connector = new InfluxDBConnector( settings );

  callback && process.nextTick(callback);
}


function InfluxDBConnector(settings) {
  this.settings = settings;

  this.clusterHosts = this.settings.hosts || [{
      host: this.settings.host || 'localhost',
      port: this.settings.port || 8086,
      protocol: this.settings.protocol || 'http'
    }];

  this.client = influx({
    //cluster configuration
    hosts : this.clusterHosts,
    // or single-host configuration
    username : this.settings.username,
    password : this.settings.password,
    database : this.settings.database,

    failoverTimeout: this.settings.failoverTimeout || 60000,
    maxRetries: this.settings.maxRetries || 3,
    timePrecision: this.settings.timePrecision || 'ms'
  });
}

exports.InfluxDBConnector = InfluxDBConnector;


InfluxDBConnector.prototype.connect = function (cb) {
};


InfluxDBConnector.prototype.disconnect = function (cb) {
};


/**
 * Find an instance of a given model/id
 * @param {string} model The model name
 * @param {*} id The id value
 * @param {function} [callback] The callback function
 */
InfluxDBConnector.prototype.find = function find(model, id, callback) {
  callback( 'Not Implemented' );
};


var operators = {
  eq: '=',
  lt: '<',
  gt: '>',
  gte: '>=',
  lte: '<='
};

/**
 * Query all instances for a given model based on the filter
 * @param {string} model The model name
 * @param {object} filter The filter object
 * @param {function} [callback] The callback function
 */
InfluxDBConnector.prototype.all = function all(model, filter, callback) {
  var query = 'SELECT * FROM ' + model;
  if( filter.where ) {
    query += ' WHERE ';
    var whereClause = [];
    Object.keys(filter.where).forEach( function(k, i){
      if( typeof filter.where[k] == 'object' ) {
        for( var p in filter.where[k] ) {
          if( operators[p] ) {
            whereClause.push( '"' + k + '"' + operators[p] + "'" + filter.where[k][p] + "'" );
          }
        }
      } else {
        whereClause.push( '"' + k + '"=\'' + filter.where[k] + "'" );
      }

    });
    query += whereClause.join( ' AND ' );
  }

  debug( "InfluxDB Query ", query );

  if( filter.raw ) {
    this.client.queryRaw( query, function(err, results) {
      callback && callback( err, results );
    });
  } else if( filter.continuous ) {
    this.client.createContinuousQuery('testQuery', 'SELECT count(value) INTO valuesCount_1h FROM presence WHERE time > now() - 980h GROUP BY time(1h)', function (err, res) {
      callback && callback( err, res );
    });
  } else {
    this.client.query( query, function(err, results) {
      if( !err && results ) {
        results = results.map( function( r ){
          return { results: r };
        });
      }
      callback && callback( err, results );
    });
  }


};


/**
 * Get types associated with the connector
 * @returns {String[]} The types for the connector
 */
InfluxDBConnector.prototype.getTypes = function() {
  return ['influxdb'];
};