var core, config, logger, loggerQuery,
    _ = require('lodash'), m = require('mysql'),
    poolCluster = m.createPoolCluster(), hasSlave = false;
var group = {
  MASTER: 1,
  SLAVE: 2
};
var funcGetConnection = function(poolGroup, callback) {
  if (poolGroup == group.SLAVE) {
    poolGroup = hasSlave ? 'SLAVE*' : 'MASTER';
  } else {
    poolGroup = 'MASTER';
  }
  poolCluster.getConnection(poolGroup, function(error, connection) {
    mysql.assert(error);
    callback(connection);
  });
};

var mysql = {
  assert: function(error) {
    if (error) {
      logger.error(error);
      throw '[mysql] ' + error;
    }
  },
  init: function(c, callback) {
    core = c;
    // logger = core.getLogger('mysql');
    // loggerQuery = core.getLogger('mysql-query');
    config = core.getConfig('mysql');
    if (config.master) {
      poolCluster.add('MASTER', config.master);
    } else {
      throw '找不到 MASTER 配置';
    }
    if (config.slave && _.isArray(config.slave) && config.slave.length) {
      _.each(config.slave, function(slave, index) {
        poolCluster.add('SLAVE-' + (index + 1), slave);
      });
      hasSlave = true;
    }
    callback();
  },
  uninit: function() {
    poolCluster.end();
  },
  format: m.format,
  query: function(sql, callback) {
    var isRead = /^\s*(?:SELECT|SHOW)\s/i.test(sql),
        poolGroup = isRead ? group.SLAVE : group.MASTER;
    funcGetConnection(poolGroup, function(connection) {
      var timeStart = new Date().getTime();
      connection.query(sql, function(error, result) {
        var timeDiff = (new Date().getTime() - timeStart).toString();
        connection.release();
        if (config.log_query) {
          loggerQuery.info('(' + timeDiff + 'ms) [' + sql + '] (' + timeDiff + 'ms)');
        }
        mysql.assert(error);
        return typeof callback === 'function' ? callback(result): null;
      });
    });
  }
};

module.exports = mysql;
