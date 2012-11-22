/*
 * couchdb/index.js: CouchDB engine wrapper
 *
 * (C) 2011 Nodejitsu Inc.
 * MIT LICENCE
 *
 */

var url = require('url'),
    cradle = require('cradle'),
    resourceful = require('../../../resourceful'),
    render = require('./view').render,
    filter = require('./view').filter;

var Couchdb = exports.Couchdb = function Couchdb(config) {
  if (config.uri) {
    var parsed = url.parse(config.uri);
    config.uri = parsed.hostname;
    config.port = parseInt(config.port || parsed.port, 10);
    config.database = config.database || ((parsed.pathname || '').replace(/\//g, ''));
  }

  this.connection = new(cradle.Connection)({
    host:  config.host || config.uri || '127.0.0.1',
    port:  config.port || 5984,
    raw:   true,
    cache: false,
    secure: config.secure,
    auth:  config && config.auth || null
  }).database(config.database || resourceful.env);

  this.cache = new resourceful.Cache();
};

Couchdb.prototype.protocol = 'couchdb';

Couchdb.prototype.load = function (data) {
  throw new(Error)("Load not valid for couchdb engine.");
};

Couchdb.prototype.request = function (method) {
  var args = Array.prototype.slice.call(arguments, 1);
  return this.connection[method].apply(this.connection, args);
};

Couchdb.prototype.head = function (id, callback) {
  return this.request('head', id, callback);
};

Couchdb.prototype.get = function (id, callback) {
  return this.request.call(this, 'get', id, function (e, res) {
    if (e) {
      if (e.headers) {
        e.status = e.headers.status;
      }
      return callback(e);
    }

    if (Array.isArray(id)) {
      res = res.rows.map(function (r) {
        if (r.doc) {
          r.doc.id = r.doc._id;
          delete r.doc._id;
        }
        return r.doc;
      });
    } else {
      res.id = res._id;
      delete res._id;
    }

    return callback(null, res);
  });
};

Couchdb.prototype.put = function (id, doc, callback) {
  delete doc.id;
  return this.request('put', id, doc, function (e, res) {
    if (e) {
      if (e.headers) {
        e.status = e.headers.status;
      }
      return callback(e);
    }

    doc._rev = res.rev;
    doc.id = id;
    callback(null, doc);
  });
};

Couchdb.prototype.post = Couchdb.prototype.create = function (doc, callback) {
  return this.request('post', doc, function (e, res) {
    if (e) return callback(e);

    doc.id = res.id;
    doc._rev = res.rev;
    callback(null, doc);
  });
};

Couchdb.prototype.save = function (id, doc, callback) {
  var args = Array.prototype.slice.call(arguments, 0),
      callback = args.pop(),
      doc = args.pop();

  // if there's an ID left in args after popping off the callback and
  // the doc, then we need to PUT, otherwise create a new record thru POST
  if (args.length) {
    return this.put.apply(this, arguments);
  }

  // checks for presence of _id in doc, just in case the caller forgot
  // to add an id as first argument
  if (doc.id) {
    return this.put.apply(this, [doc.id, doc, callback]);
  } else {
    return this.post.call(this, doc, callback);
  }
};

Couchdb.prototype.update = function (id, doc, callback) {
 if (this.cache.has(id)) {
   var r = this.cache.get(id);
   this.put(id, resourceful.mixin({}, r, doc), callback)
 } else {
   var that = this;
   this.request('merge', id, doc, function (err, res) {
     that.get(id, callback);
   });
 }
};

Couchdb.prototype.destroy = function (id, callback) {
  var that = this, args, rev,
      cb = callback;

  callback = function (err, res) {
    if (err) return cb(err);
    cb(null, that.cache.get(id) || { id: res.id, _rev: res.rev });
  };

  args = [id, callback];

  if (Array.isArray(id)) {
    rev = id[1];
    id = id[0];
  }

  if (rev) {
    return this.request.apply(this, ['remove', id, rev, callback]);
  }
  if (this.cache.has(id)) {
    args.splice(1, -1, this.cache.get(id)._rev);
    args[0] = id;
    return this.request.apply(this, ['remove'].concat(args));
  }

  this.head(id, function (e, headers, res) {
    if (res === 404 || !headers['etag']) {
      e = e || { reason: 'not_found', status: 404 };
    }

    if (headers.etag) {
      args.splice(1, -1, headers.etag.slice(1, -1));
      return that.request.apply(that, ['remove'].concat(args));
    } else { args.pop()(e) }
  });
};

Couchdb.prototype.view = function (path, opts, callback) {
	if(typeof opts === 'function'){
		callback=opts;
		opts=null;
	}
  return this.request.call(this, 'view', path, opts, function (e, res) {
    if (e) return callback(e);

    callback(null, res.rows.map(function (r) {
      // With `include_docs=true`, the 'doc' attribute is set instead of 'value'.
      if (r.doc) {
        if (r.doc._id) {
          r.doc.id = r.doc._id.split('/').slice(1).join('/');
          delete r.doc._id;
        }
        return r.doc;
      } else {
        if (r.value._id) {
          r.value.id = r.value._id.split('/').slice(1).join('/');
          delete r.value._id;
        }
        return r.value;
      }
    }));
  });
};

Couchdb.prototype.list = function (path, opts, callback) {
	if(typeof opts === 'function'){
		callback=opts;
		opts=null;
	}
  return this.request.call(this, 'list', path, opts, function (e, res) {
    if (e) return callback(e);

    callback(null, (res.rows ? res.rows : res).map(function (r) {
      // With `include_docs=true`, the 'doc' attribute is set instead of 'value'.
      if (r.doc) {
        if (r.doc._id) {
          r.doc.id = r.doc._id.split('/').slice(1).join('/');
          delete r.doc._id;
        }
        return r.doc;
      } else {
        if (r.value._id) {
          r.value.id = r.value._id.split('/').slice(1).join('/');
          delete r.value._id;
        }
        return r.value;
      }
    }));
  });
};

Couchdb.prototype.find = function(conditions, callback) {
	this.connection.temporaryView(resourceful.render({
		map: function(doc) {
			var obj = $conditions;
			if (function() {

				function get_value(object, key) {
				  if (key.indexOf(".") !== -1) {
				    return eval_("(function(){ return object." + key + ";})()");
				  } else {
				    return object[key];
				  }
				};

				function resolve_date(value_in) {
				  var is_a_date;
				  if (value_in instanceof Date) {
				    return value_in;
				  }
				  if (value_in instanceof String) {
				    is_a_date = false;
				    try {
				      is_a_date = !isNaN(Date.parse(value_in));
				    } catch (e) {
				      is_a_date = false;
				    }
				    if (is_a_date) {
				      return new Date(Date.parse(value_in));
				    }
				  }
				  return value_in;
				};

				function compare_values(value_in, expected_value, comparer) {
				  var value, _i, _j, _len, _len1;
				  value_in = resolve_date(value_in);
				  expected_value = resolve_date(expected_value);
				  switch (comparer) {
				    case "is_null":
				      return value_in === null || value_in === void 0;
				    case "is_not_null":
				      return value_in !== null && value_in !== void 0;
				    case "is_in":
				      for (_i = 0, _len = expected_value.length; _i < _len; _i++) {
				        value = expected_value[_i];
				        if (compare_values(value_in, value, "==")) {
				          return true;
				        }
				      }
				      return false;
				    case "is_not_in":
				      for (_j = 0, _len1 = expected_value.length; _j < _len1; _j++) {
				        value = expected_value[_j];
				        if (compare_values(value_in, value, "==")) {
				          return false;
				        }
				      }
				      return true;
				    case "==":
				      return value_in === expected_value;
				    case ">":
				      return value_in > expected_value;
				    case ">=":
				      return value_in >= expected_value;
				    case "<":
				      return value_in < expected_value;
				    case "<=":
				      return value_in <= expected_value;
				    case "~":
				      return new RegExp(expected_value).test((value_in || "").toString());
				  }
				};

				function compare(obj_value, expected_value) {
				  var index, k, out, value, _i, _j, _len, _len1, _ref, _ref1;
				  if (typeof expected_value === "object") {
				    if ((expected_value.$or != null) && typeof expected_value.$or === "object" && expected_value.$or instanceof Array) {
				      out = false;
				      _ref = expected_value.$or;
				      for (index = _i = 0, _len = _ref.length; _i < _len; index = ++_i) {
				        value = _ref[index];
				        out = out || compare(doc, value);
				      }
				      return out;
				    }
				    if ((expected_value.$and != null) && typeof expected_value.$and === "object" && expected_value.$and instanceof Array) {
				      _ref1 = expected_value.$and;
				      for (index = _j = 0, _len1 = _ref1.length; _j < _len1; index = ++_j) {
				        value = _ref1[index];
				        if (!compare(doc, value)) {
				          return false;
				        }
				      }
				      return true;
				    }
				    if (expected_value.$in != null) {
				      return compare_values(obj_value, expected_value.$in, "is_in");
				    }
				    if (expected_value.$notin != null) {
				      return compare_values(obj_value, expected_value.$notin, "_is_not_in");
				    }
				    if (expected_value.$isnull != null) {
				      return compare_values(obj_value, null, "is_null");
				    }
				    if (expected_value.$notnull != null) {
				      return compare_values(obj_value, null, "is_not_null");
				    }
				    if (expected_value.$lt != null) {
				      return compare_values(obj_value, expected_value.$lt, "<");
				    }
				    if (expected_value.$lte != null) {
				      return compare_values(obj_value, expected_value.$lte, "<=");
				    }
				    if (expected_value.$gt != null) {
				      return compare_values(obj_value, expected_value.$gt, ">");
				    }
				    if (expected_value.$gte != null) {
				      return compare_values(obj_value, expected_value.$gte, ">=");
				    }
				    if (expected_value.$like != null) {
				      return compare_values(obj_value, expected_value.$like, "~");
				    }
				    if (expected_value instanceof RegExp) {
				      return compare_values(obj_value, expected_value.toString(), "~");
				    } else {
				      for (k in expected_value) {
				        if (!compare(get_value(doc, k), expected_value[k])) {
				          return false;
				        }
				      }
				      return true;
				    }
				  }
				  return compare_values(obj_value, expected_value, "==");
				};
				return compare(doc, obj);
			}()) {
				emit(doc._id, doc);
			}
		}
	}, {
		conditions: JSON.stringify(conditions)
	}), function(e, res) {
		if (e) return callback(e);
		callback(null, res.rows.map(function(r) {
			// With `include_docs=true`, the 'doc' attribute is set instead of 'value'.
			if (r.doc) {
				if (r.doc._id) {
					r.doc.id = r.doc._id.split('/')
						.slice(1)
						.join('/');
					delete r.doc._id;
				}
				return r.doc;
			} else {
				if (r.value._id) {
					r.value.id = r.value._id.split('/')
						.slice(1)
						.join('/');
					delete r.value._id;
				}
				return r.value;
			}
		}));
	});
};


Couchdb.prototype.filter = function (name, data) {
  return filter.call(data.resource, name, data.options, data.filter);
};

Couchdb.prototype.sync = function (factory, callback) {
  var that = this,
      id = '_design/' + factory.resource;

  factory._design = factory._design || {};
  factory._design._id = id;
  if (factory._design._rev) return callback(null);

  this.connection.head(id, function (e, headers, status) {
    if (!e && headers.etag) {
      factory._design._rev = headers.etag.slice(1, -1);
    }

    that.connection.put(id, factory._design, function (e, res) {
      if (e) {
        if (e.reason === 'no_db_file') {
          that.connection.create(function () {
            that.sync(factory, callback);
          });
        }
        else {

          /* TODO: Catch errors here. Needs a rewrite, because of the race */
          /* condition, when the design doc is trying to be written in parallel */
          callback(e);
        }
      }
      else {
        // We might not need to wait for the document to be
        // persisted, before returning it. If for whatever reason
        // the insert fails, it'll just re-attempt it. For now though,
        // to be on the safe side, we wait.
        factory._design._rev = res.rev;
        callback(null, factory._design);
      }
    });
  });
};

//
// Relationship hook
//
Couchdb._byParent = function (factory, rfactory) {
  var conn = this
    , parent = rfactory.lowerResource
    , child = factory.lowerResource;

  factory['by' + rfactory.resource] = function (id, callback) {
    rfactory.get.call(rfactory, id, function (err, res) {
      if (err) return callback(err);

      var children = res[child + '_ids'].map(function (e) {
        return parent + "/" + id + "/" + e;
      });

      factory.get.call(factory, children, callback);
    });
  };
};
