/*!
 * Module dependencies
 */
var ml = require('marklogic');
var util = require('util');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:ml');
var qb = {};

/*!
 * Generate the MarkLogic connection information object from the options
 */
function generateConnectionObject(options) {
  options.hostname = (options.hostname || options.host || '127.0.0.1');
  options.port = (options.port || 8000);
  options.database = (options.database || options.db || 'documents');
  options.authType = (options.authType || 'DIGEST');
  var username = options.username || options.user;
  return {
    host: options.hostname,
    port: options.port,
    user: options.username,
    password: options.password,
    authType: options.authType
  };
}

/**
 * Initialize the MarkLogic connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!ml) {
    return;
  }

  var s = dataSource.settings;

  s.connInfo = s.connInfo || generateConnectionObject(s);
  dataSource.connector = new MarkLogic(s, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

/**
 * The constructor for MarkLogic connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function MarkLogic(settings, dataSource) {
  Connector.call(this, 'ml', settings);

  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  this.dataSource = dataSource;

}

util.inherits(MarkLogic, Connector);

/**
 * Connect to MongoDB
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The mongo DB object
 */
MarkLogic.prototype.connect = function (callback) {
  var self = this;
  if (self.db) {
    process.nextTick(function () {
      callback && callback(null, self.db);
    });
  } else {
    debug('Connecting.', self.settings.connInfo);
    self.db = ml.createDatabaseClient(self.settings.connInfo);
    self.qb = ml.queryBuilder;
    debug('Connected.', self.db);
    callback && callback(null, self.db);
  }
};

MarkLogic.prototype.getTypes = function () {
  return ['db', 'nosql', 'ml', 'marklogic'];
};

/**
 * Get collection name for a given model
 * @param {String} model Model name
 * @returns {String} collection name
 */
MarkLogic.prototype.collectionName = function(model) {
  var modelClass = this._models[model];
  if (modelClass.settings.ml) {
    model = modelClass.settings.ml.collection || model;
  }
  return model;
};

/**
 * Access a MarkLogic collection by model name
 * @param {String} model The model name
 * @returns {*}
 */
MarkLogic.prototype.collection = function (model) {
  if (!this.db) {
    throw new Error('MarkLogic connection is not established');
  }
  var collectionName = this.collectionName(model);
  return this.db.queryCollection(collectionName);
};

/**
 * Create a new model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.create = function (model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('create', model, data);
  }
  var doc = {
    extension: 'json',
    directory: model + '/',
    content: data,
    contentType: 'application/json',
    collections: [ model ]
  };
  this.db.documents.write(doc).result().then(function(response) {
    if (self.debug) {
      debug('create.callback', model, response);
    }
    data.id = response.documents[0].uri;
    callback(null, data.id);
  });
};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.save = function (model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('save', model, data);
  }
  var doc = {
    uri: data.id,
    content: data,
    contentType: 'application/json'
  };
  this.db.documents.write(doc).result().then(function(response) {
    if (self.debug) {
      debug('save.callback', model, response);
    }
    data.id = response.documents[0].uri;
    callback(null, data.id);
  });
};


/*!
 * Decide if uri should be included
 * @param {Object} fields
 * @returns {Boolean}
 * @private
 */
function idIncluded(fields, idName) {
  if (!fields) {
    return true;
  }
  if (Array.isArray(fields)) {
    return fields.indexOf(idName) >= 0;
  }
  if (fields[idName]) {
    // Included
    return true;
  }
  if ((idName in fields) && !fields[idName]) {
    // Excluded
    return false;
  }
  for (var f in fields) {
    return !fields[f]; // If the fields has exclusion
  }
  return true;
}

MarkLogic.prototype.buildWhere = function (model, where) {
  var self = this;
  var query = {};
  if (where === null || (typeof where !== 'object')) {
    return query;
  }
  var idName = self.idName(model);
  Object.keys(where).forEach(function (k) {
    var cond = where[k];
    if (k === idName) {
      k = 'id';
    }
    if (k === 'and' || k === 'or' || k === 'nor') {
      if (Array.isArray(cond)) {
        cond = cond.map(function (c) {
          return self.buildWhere(model, c);
        });
      }
      query['$' + k ] = cond;
      delete query[k];
      return;
    }
    var spec = false;
    var options = null;
    if (cond && cond.constructor.name === 'Object') {
      options = cond.options;
      spec = Object.keys(cond)[0];
      cond = cond[spec];
    }
    if (spec) {
      if (spec === 'between') {
        query[k] = { $gte: cond[0], $lte: cond[1]};
      } else if (spec === 'inq') {
        query[k] = { $in: cond.map(function (x) {
          if ('string' !== typeof x) return x;
          return x;
        })};
      } else if (spec === 'like') {
        query[k] = {$regex: new RegExp(cond, options)};
      } else if (spec === 'nlike') {
        query[k] = {$not: new RegExp(cond, options)};
      } else if (spec === 'neq') {
        query[k] = {$ne: cond};
      }
      else {
        query[k] = {};
        query[k]['$' + spec] = cond;
      }
    } else {
      if (cond === null) {
        query[k] = {$type: 10};
      } else {
        query[k] = cond;
      }
    }
  });
  return query;
}

/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.all = function all(model, filter, options, callback) {
  var self = this;
  if (self.debug) {
    debug('all', model, filter);
  }
  filter = filter || {};
  var idName = self.idName(model);
  var query = {};
  if (filter.where) {
    if (filter.where[idName]) {
      if (self.debug) {
        debug('all - ID Search', filter.where[idName]);
      }
      var uri = filter.where[idName];
      delete filter.where[idName];
      filter.where._uri = unescape(uri);
    }
    query = self.buildWhere(model, filter.where);
  }
  var fields = filter.fields;
  if (filter.where && filter.where._uri) {
    self.db.documents.read(filter.where._uri).result().then(function(response) {
      processResponse(response);
    })
  } else if (fields) {
    self.db.queryCollection(model).result().then(function(response) {
      processResponse(response);
    });
  } else {
    var criteria = self.qb.where(self.qb.byExample(query));
    self.db.queryCollection(model, criteria).result().then(function(response) {
      processResponse(response);
    });
  }

  function processResponse(cursor) {
      var results = [];
      cursor.forEach(function(entry) {
        entry.content.id = entry.uri;
        results.push(entry.content);
      });
      callback(null, results);
  }
};

/**
 * Delete all instances for the given model
 * @param {String} model The model name
 * @param {Object} [where] The filter for where
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.destroyAll = function destroyAll(model, where, options, callback) {
  var self = this;
  if (self.debug) {
    debug('destroyAll', model, where);
  }
  if (!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }
  where = self.buildWhere(model, where);

  if (where && where.id) {
    self.db.documents.remove(unescape(where.id)).result().then(function(response) {
      callback && callback(null, {count: response.uris.length});
    });
  }

/*
  this.execute(model, 'remove', where || {}, function(err, info) {
    if (err) return callback && callback(err);

    if (self.debug)
      debug('destroyAll.callback', model, where, err, info);

    var affectedCount = info.result ? info.result.n : undefined;

    callback && callback(err, {count: affectedCount});
  });
*/
};

/**
 * Count the number of instances for the given model
 *
 * @param {String} model The model name
 * @param {Function} [callback] The callback function
 * @param {Object} filter The filter for where
 *
 */
MarkLogic.prototype.count = function count(model, filter, options, callback) {
  var self = this;
  if (self.debug) {
    debug('count', model, filter, options);
  }
  query = self.buildWhere(model, filter);
  if (filter && filter.id) {
    self.db.documents.read(unescape(filter.id)).result().then(function(response) {
      callback(null, response.length);
    });
  } else {
    var criteria = self.qb.where(self.qb.byExample(query));
    self.db.queryCollection(model, criteria).result().then(function(response) {
      callback(null, response.length);
    });
  }
};

/**
 * Update properties for the model instance data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.updateAttributes = function updateAttrs(model, uri, data, options, cb) {
  var self = this;

  if (self.debug) {
    debug('updateAttributes', model, uri, data);
  }
  var idName = this.idName(model);

  data.id = uri;
  self.save(model, data, options, cb);

};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
MarkLogic.prototype.update =
  MarkLogic.prototype.updateAll = function updateAll(model, where, data, options, cb) {
    var self = this;
    if (self.debug) {
      debug('updateAll', model, where, data);
    }
    self.all(model, where, options, function(err, response) {
      if (response[0]) {
        data.id = response[0].id;
        self.save(model, data, options, cb);
      }
    });
  };

/**
 * Disconnect from MarkLogic
 */
MarkLogic.prototype.disconnect = function (cb) {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.db) {
    this.db.close();
  }
  if (cb) {
    process.nextTick(cb);
  }
};


MarkLogic.prototype.ping = function (cb) {
  var self = this;
  if (self.db) {
    this.db.collection('dummy').findOne({id: 1}, cb);
  } else {
    self.dataSource.once('connected', function () {
      self.ping(cb);
    });
    self.dataSource.once('error', function (err) {
      cb(err);
    });
    self.connect(function() {});
  }
};

