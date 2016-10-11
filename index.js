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
  assert: (error, sql) => {
    if (error) {
      if (sql) {
        logger.error('Error sql: [' + sql + ']');
      }
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
  query: function(sql, params, callback) {
    if (arguments.length <= 2) {
      callback = params;
    } else {
      sql = m.format(sql, params);
    }
    let isWrite = /^\s*(?:SET|INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX)\s/i.test(sql),
        poolGroup = isWrite ? GROUP_MASTER : GROUP_SLAVE;
    funcGetConnection(poolGroup, (conn) => {
      let timer = new Date().getTime();
      conn.query(sql, (error, result) => {
        timer = new Date().getTime() - timer;
        conn.release();
        if (config.log_query) {
          loggerQuery.info('(' + timer.toString() + 'ms) [' + sql + ']');
        }
        mysql.assert(error, sql);

        return typeof callback === 'function' ? callback({
          sql: sql,
          timer: timer,
          result: result
        }) : null;
      });
    });
  }
};

module.exports = mysql;
