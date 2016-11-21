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
  debug( "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" );
  debug( "this.clusterHosts ", this.client );
  debug( "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" );
};


InfluxDBConnector.prototype.disconnect = function (cb) {
};


InfluxDBConnector.prototype.create = function all(model, data, callback) {
  var point = {};
  if(data.value !== null) {
    point.value = data.value;
    delete data.value;
  }
  if(data.time !== null) {
    point.time = data.time;
    delete data.time;
  }
  var points = [[point, data]];
  this.client.writePoints(model, points, function (err) {
    callback(err);
  });
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

InfluxDBConnector.prototype.buildWhere = function( obj ) {
  var self = this;
  return '(' + Object.keys(obj).map(function (k) {
      var whereClause = [];
      if (k === 'and' || k === 'or') {
        if (Array.isArray(obj[k])) {
          return '(' + obj[k].map(function (condObj) {
              return self.buildWhere(condObj);
            }).join(' ' + k + ' ') + ')';
        }
      } else {
        if (typeof obj[k] == 'object') {
          for (var p in obj[k]) {
            if (operators[p]) {
              var val = obj[k][p];
              if( !isNaN(val) && k == 'time' ) {
                val = Math.floor( val /= 1000 );
                val += 's';
              } else {
                val = "'" + val + "'";
              }
              whereClause.push('"' + k + '"' + operators[p] + val);
            }
          }
        } else {
          var val2 = obj[k];
          if( !isNaN(val2) && k == 'time' ) {
            val2 = Math.floor( val2 /= 1000 );
            val2 += 's';
          } else {
            val2 = "'" + val2 + "'";
          }
          whereClause.push('"' + k + '"=' + val2);
        }
        return '(' + whereClause.join(' AND ') + ')';
      }
    }).join( ' AND ' ) + ')';
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
    query += ' WHERE ' + this.buildWhere( filter.where );
  }

  if( filter.limit ) {
    query += ' LIMIT ' + filter.limit;
  }

  if( filter.offset ) {
    query += ' OFFSET ' + filter.offset;
  }

  if( filter.groupBy ) {
    query += ' GROUP BY ' + filter.groupBy;
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
