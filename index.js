"use strict";
const GROUP_MASTER = 1, GROUP_SLAVE = 2;
let core, config, logger, loggerQuery,
    _ = require('lodash'), m = require('mysql'),
    poolCluster = m.createPoolCluster(), hasSlave = false;
let funcGetConnection = (poolGroup, callback) => {
  if (poolGroup == GROUP_SLAVE) {
    poolGroup = hasSlave ? 'SLAVE*' : 'MASTER';
  } else {
    poolGroup = 'MASTER';
  }
  poolCluster.getConnection(poolGroup, (error, connection) => {
    mysql.assert(error);
    callback(connection);
  });
};

let serviceName = 'mysql';
let mysql = {
  assert: (error) => {
    if (error) {
      logger.error(error);
      throw '[' + serviceName + '] ' + error;
    }
  },
  init: (name, c, callback) => {
    serviceName = name;
    core = c;
    logger = core.getLogger(serviceName);
    loggerQuery = core.getLogger(serviceName + '-query');
    config = core.getConfig(serviceName);
    if (config.master) {
      poolCluster.add('MASTER', config.master);
    } else {
      throw 'MASTER config not found.';
    }
    if (config.slave && _.isArray(config.slave) && config.slave.length) {
      _.forEach(config.slave, (slave, index) => {
        poolCluster.add('SLAVE-' + (index + 1), slave);
      });
      hasSlave = true;
    }
    if (!config.enable_api) {
      // disable query api
      delete mysql.post_query;
    }
    callback();
  },
  uninit: () => {
    poolCluster.end();
  },
  format: m.format,
  post_query: (req, res, next) => {
    if (!req.body || req.body.sql === undefined) {
      throw 'Params is wrong';
    }
    mysql.query(req.body.sql, next);
  },
  query: (sql, params, callback) => {
    if (arguments.length <= 2) {
      callback = params;
    } else {
      sql = m.format(sql, params);
    }
    let isRead = /^\s*(?:SELECT|SHOW)\s/i.test(sql),
        poolGroup = isRead ? GROUP_SLAVE : GROUP_MASTER;
    funcGetConnection(poolGroup, (conn) => {
      let timer = new Date().getTime();
      conn.query(sql, (error, result) => {
        timer = (new Date().getTime() - timer).toString();
        conn.release();
        if (config.log_query) {
          loggerQuery.info('(' + timer + 'ms) [' + sql + ']');
        }
        mysql.assert(error);
        return typeof callback === 'function' ? callback(result): null;
      });
    });
  }
};

module.exports = mysql;
