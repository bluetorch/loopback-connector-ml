/*!
 * Module dependencies
 */
var ml = require('marklogic');
var util = require('util');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:ml');

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
 * Execute a MarkLogic command
 * @param {String} model The model name
 * @param {String} command The command name
 * @param [...] params Parameters for the given command
 */
MarkLogic.prototype.execute = function(model, command) {
  var collection = this.collection(model);
  // Get the parameters for the given command
  var args = [].slice.call(arguments, 2);
  // The last argument must be a callback function
  var callback = args[args.length - 1];
  var context = {
    model: model,
    collection: collection, req: {
      command: command,
      params: args
    }
  };
  this.notifyObserversAround('execute', context, function(context, done) {
    args[args.length - 1] = function(err, result) {
      if (err) {
        debug('Error: ', err);
      } else {
        context.res = result;
        debug('Result: ', result);
      }
      done(err, result);
    }
    debug('MarkLogic: model=%s command=%s', model, command, args);
    return collection[command].apply(collection, args);
  }, callback);
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
  var idValue = self.getIdValue(model, data);
  var idName = self.idName(model);

  this.execute(model, 'insert', data, function (err, result) {
    if (self.debug) {
      debug('create.callback', model, err, result);
    }
    if(err) {
      return callback(err);
    }
    idValue = result.documents[0].uri;
    var modelClass = self._models[model];
    var idType = modelClass.properties[idName].type;
    if (idType === String) {
      idValue = String(idValue);
    }
    data[idName] = idValue;
    callback(err, err ? null : idValue);
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
  var idValue = self.getIdValue(model, data);
  var idName = self.idName(model);

  this.execute(model, 'save', data,  function (err, result) {
    if (!err) {
      self.setIdValue(model, data, idValue);
      idName !== '_uri' && delete data._uri;
    }
    if (self.debug) {
      debug('save.callback', model, err, result);
    }

    var info = {};
    if (result && result.result) {
      // create result formats:
      //   { ok: 1, n: 1, upserted: [ [Object] ] }
      //   { ok: 1, nModified: 0, n: 1, upserted: [ [Object] ] }
      //
      // update result formats:
      //   { ok: 1, n: 1 }
      //   { ok: 1, nModified: 1, n: 1 }
      if (result.result.ok === 1 && result.result.n === 1) {
        info.isNewInstance = !!result.result.upserted;
      } else {
        debug('save result format not recognized: %j', result.result);
      }
    }

    callback && callback(err, result && result.ops, info);
  });
};

/**
 * Check if a model instance exists by uri
 * @param {String} model The model name
 * @param {*} uri The uri value
 * @param {Function} [callback] The callback function
 *
 */
MarkLogic.prototype.exists = function (model, uri, options, callback) {
  var self = this;
  if (self.debug) {
    debug('exists', model, uri);
  }
  this.execute(model, 'findOne', {_uri: uri}, function (err, data) {
    if (self.debug) {
      debug('exists.callback', model, uri, err, data);
    }
    callback(err, !!(!err && data));
  });
};

/**
 * Find a model instance by uri
 * @param {String} model The model name
 * @param {*} uri The uri value
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.find = function find(model, uri, options, callback) {
  var self = this;
  if (self.debug) {
    debug('find', model, uri);
  }
  var idValue = uri;
  var idName = self.idName(model);

  this.execute(model, 'findOne', {_uri: uri}, function (err, data) {
    if (self.debug) {
      debug('find.callback', model, uri, err, data);
    }

    data = self.fromDatabase(model, data);
    data && idName !== '_uri' && delete data._uri;
    callback && callback(err, data);
  });
};

/**
 * Parses the data input for update operations and returns the
 * sanitised version of the object.
 *
 * @param data
 * @returns {*}
 */
MarkLogic.prototype.parseUpdateData = function(model, data) {
  var parsedData = {};

  if (this.settings.allowExtendedOperators === true) {
    // Check for other operators and sanitize the data obj
    var acceptedOperators = [
      // Field operators
      '$currentDate', '$inc', '$max', '$min', '$mul', '$rename', '$setOnInsert', '$set', '$unset',
      // Array operators
      '$addToSet', '$pop', '$pullAll', '$pull', '$pushAll', '$push',
      // Bitwise operator
      '$bit'
    ];

    var usedOperators = 0;

    // each accepted operator will take its place on parsedData if defined
    for (var i = 0; i < acceptedOperators.length; i++) {
      if(data[acceptedOperators[i]]) {
        parsedData[acceptedOperators[i]] = data[acceptedOperators[i]];
        usedOperators++;
      }
    }

    // if parsedData is still empty, then we fallback to $set operator
    if(usedOperators === 0) {
      parsedData.$set = data;
    }
  } else {
    parsedData.$set = data;
  }

  return parsedData;
};

/**
 * Update if the model instance exists with the same uri or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.updateOrCreate = function updateOrCreate(model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('updateOrCreate', model, data);
  }

  var id = self.getIdValue(model, data);
  var idName = self.idName(model);
  delete data[idName];

  // Check for other operators and sanitize the data obj
  data = self.parseUpdateData(model, data);

  this.execute(model, 'findAndModify', {
    _uri: uri
  }, [
    ['_uri', 'asc']
  ], data, {upsert: true, new: true}, function (err, result) {
    if (self.debug) {
      debug('updateOrCreate.callback', model, uri, err, result);
    }
    var object = result && result.value;
    if (!err && !object) {
      // No result
      err = 'No ' + model + ' found for uri ' + uri;
    }
    if (!err) {
      self.setIdValue(model, object, uri);
      object && idName !== '_uri' && delete object._uri;
    }

    var info;
    if (result && result.lastErrorObject) {
      info = { isNewInstance: !result.lastErrorObject.updatedExisting };
    } else {
      debug('updateOrCreate result format not recognized: %j', result);
    }

    callback && callback(err, object, info);
  });
};

/**
 * Delete a model instance by uri
 * @param {String} model The model name
 * @param {*} uri The uri value
 * @param [callback] The callback function
 */
MarkLogic.prototype.destroy = function destroy(model, uri, options, callback) {
  var self = this;
  if (self.debug) {
    debug('delete', model, uri);
  }
  this.execute(model, 'remove', {_uri: uri}, function (err, result) {
    if (self.debug) {
      debug('delete.callback', model, uri, err, result);
    }
    callback && callback(err, result && result.ops);
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
      k = '_uri';
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
        // http://docs.mongodb.org/manual/reference/operator/query/type/
        // Null: 10
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
      var uri = filter.where[idName];
      delete filter.where[idName];
      filter.where._uri = uri;
    }
    query = self.buildWhere(model, filter.where);
  }
  var fields = filter.fields;
  if (fields) {
    this.execute(model, 'find', query, fields, processResponse);
  } else {
    this.execute(model, 'find', query, processResponse);
  }

  function processResponse(err, cursor) {
    if (err) {
      return callback(err);
    }
    var order = {};
    if (!filter.order) {
      var idNames = self.idNames(model);
      if (idNames && idNames.length) {
        filter.order = idNames;
      }
    }
    if (filter.order) {
      var keys = filter.order;
      if (typeof keys === 'string') {
        keys = keys.split(',');
      }
      for (var index = 0, len = keys.length; index < len; index++) {
        var m = keys[index].match(/\s+(A|DE)SC$/);
        var key = keys[index];
        key = key.replace(/\s+(A|DE)SC$/, '').trim();
        if (key === idName) {
          key = '_uri';
        }
        if (m && m[1] === 'DE') {
          order[key] = -1;
        } else {
          order[key] = 1;
        }
      }
    } else {
      // order by _uri by default
      order = {_uri: 1};
    }
    cursor.sort(order);

    if (filter.limit) {
      cursor.limit(filter.limit);
    }
    if (filter.skip) {
      cursor.skip(filter.skip);
    } else if (filter.offset) {
      cursor.skip(filter.offset);
    }
    cursor.toArray(function(err, data) {
      if (self.debug) {
        debug('all', model, filter, err, data);
      }
      if (err) {
        return callback(err);
      }
      var objs = data.map(function(o) {
        if (idIncluded(fields, self.idName(model))) {
          self.setIdValue(model, o, o._uri);
        }
        // Don't pass back _uri if the fields is set
        if (fields || idName !== '_uri') {
          delete o._uri;
        }
        o = self.fromDatabase(model, o);
        return o;
      });
      if (filter && filter.include) {
        self._models[model].model.include(objs, filter.include, options, callback);
      } else {
        callback(null, objs);
      }
    });
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
  this.execute(model, 'remove', where || {}, function(err, info) {
    if (err) return callback && callback(err);

    if (self.debug)
      debug('destroyAll.callback', model, where, err, info);

    var affectedCount = info.result ? info.result.n : undefined;

    callback && callback(err, {count: affectedCount});
  });
};

/**
 * Count the number of instances for the given model
 *
 * @param {String} model The model name
 * @param {Function} [callback] The callback function
 * @param {Object} filter The filter for where
 *
 */
MarkLogic.prototype.count = function count(model, where, options, callback) {
  var self = this;
  if (self.debug) {
    debug('count', model, where);
  }
  where = self.buildWhere(model, where);
  this.execute(model, 'count', where, function (err, count) {
    if (self.debug) {
      debug('count.callback', model, err, count);
    }
    callback && callback(err, count);
  });
};

/**
 * Update properties for the model instance data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MarkLogic.prototype.updateAttributes = function updateAttrs(model, uri, data, options, cb) {
  var self = this;

  // Check for other operators and sanitize the data obj
  data = self.parseUpdateData(model, data);

  if (self.debug) {
    debug('updateAttributes', model, uri, data);
  }
  var idName = this.idName(model);

  this.execute(model, 'findAndModify', {_uri: uri}, [
    ['_uri', 'asc']
  ], data, {}, function (err, result) {
    if (self.debug) {
      debug('updateAttributes.callback', model, uri, err, result);
    }
    var object = result && result.value;
    if (!err && !object) {
      // No result
      err = 'No ' + model + ' found for uri ' + uri;
    }
    self.setIdValue(model, object, uri);
    object && idName !== '_uri' && delete object._uri;
    cb && cb(err, object);
  });
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
    var idName = this.idName(model);

    where = self.buildWhere(model, where);
    delete data[idName];

    // Check for other operators and sanitize the data obj
    data = self.parseUpdateData(model, data);

    this.execute(model, 'update', where, data, {multi: true, upsert: false},
      function(err, info) {
        if (err) return cb && cb(err);

        if (self.debug)
          debug('updateAll.callback', model, where, data, err, info);

        var affectedCount = info.result ? info.result.n : undefined;

        cb && cb(err, {count: affectedCount});
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

/**
 * Perform autoupdate for the given models. It basically calls createIndex
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
MarkLogic.prototype.autoupdate = function (models, cb) {
  var self = this;
  if (self.db) {
    if (self.debug) {
      debug('autoupdate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(self._models);

    async.each(models, function (model, modelCallback) {
      var indexes = self._models[model].settings.indexes || [];
      var indexList = [];
      var index = {};
      var options = {};

      if (typeof indexes === 'object') {
        for (var indexName in indexes) {
          index = indexes[indexName];
          if (index.keys) {
            // The index object has keys
            options = index.options || {};
            options.name = options.name || indexName;
            index.options = options;
          } else {
            options = {name: indexName};
            index = {
              keys: index,
              options: options
            };
          }
          indexList.push(index);
        }
      } else if (Array.isArray(indexes)) {
        indexList = indexList.concat(indexes);
      }

      var properties = self._models[model].properties;
      for (var p in properties) {
        if (properties[p].index) {
          index = {};
          index[p] = 1; // Add the index key
          if (typeof properties[p].index === 'object') {
            // If there is a mongodb key for the index, use it
            if (typeof properties[p].index.mongodb === 'object') {
              options = properties[p].index.mongodb;
              index[p] = options.kind || 1;

              // Backwards compatibility for former type of indexes
              if (properties[p].index.unique === true) {
                options.unique = true;
              }

            } else {
              // If there isn't an  properties[p].index.mongodb object, we read the properties from  properties[p].index
              options = properties[p].index;
            }

            if (options.background === undefined) {
              options.background = true;
            }
          // If properties[p].index isn't an object we hardcode the background option and check for properties[p].unique
          } else {
            options = {background: true};
            if(properties[p].unique) {
              options.unique = true;
            }
          }
          indexList.push({keys: index, options: options});
        }
      }

      if (self.debug) {
        debug('create indexes: ', indexList);
      }

      async.each(indexList, function (index, indexCallback) {
        if (self.debug) {
          debug('createIndex: ', index);
        }
        self.collection(model).createIndex(index.fields || index.keys, index.options, indexCallback);
      }, modelCallback);

    }, cb);
  } else {
    self.dataSource.once('connected', function () {
      self.autoupdate(models, cb);
    });
  }
};

/**
 * Perform automigrate for the given models. It drops the corresponding collections
 * and calls createIndex
 * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
MarkLogic.prototype.automigrate = function (models, cb) {
  var self = this;
  if (self.db) {
    if (self.debug) {
      debug('automigrate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(self._models);

    // Make it serial as multiple models might map to the same collection
    async.eachSeries(models, function (model, modelCallback) {

      var collectionName = self.collectionName(model);
      if (self.debug) {
        debug('drop collection %s for model %s', collectionName, model);
      }

      self.db.dropCollection(collectionName, function(err, collection) {
        if (err) {
          debug('Error dropping collection %s for model %s: ', collectionName, model, err);
          if (!(err.name === 'MongoError' && err.ok === 0
            && err.errmsg === 'ns not found')) {
            // For errors other than 'ns not found' (collection doesn't exist)
            return modelCallback(err);
          }
        }
        // Recreate the collection
        if (self.debug) {
          debug('create collection %s for model %s', collectionName, model);
        }
        self.db.createCollection(collectionName, modelCallback);
      });
    }, function (err) {
      if (err) {
        return cb && cb(err);
      }
      self.autoupdate(models, cb);
    });
  } else {
    self.dataSource.once('connected', function () {
      self.automigrate(models, cb);
    });
  }
};

MarkLogic.prototype.ping = function (cb) {
  var self = this;
  if (self.db) {
    this.db.collection('dummy').findOne({_uri: 1}, cb);
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

