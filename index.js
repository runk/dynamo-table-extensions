var async = require('async')
var dynamoTable = require('./slim')

module.exports = dynamoTable

var proto = dynamoTable.DynamoTable.prototype

// Ensures that no more than capacityRatio * writeCapacity items are written per second
proto.throttledBatchWrite = function(capacityRatio, items, cb) {
  if (!(capacityRatio > 0)) return cb(new Error('non-positive capacityRatio'))

  var self = this
  self.describeTable(function(err, info) {
    if (err) return cb(err)

    var itemsPerSecond = Math.ceil(info.ProvisionedThroughput.WriteCapacityUnits * capacityRatio);
    var written = 0;
    var ready = true;

    var waitAndWrite = function(cb) {
      async.until(function() {return ready}, function(cb) {setTimeout(cb, 10)}, function(err) {
        if (err) return cb(err)
        ready = false
        setTimeout(function() {ready = true}, 1000)

        var write = items.slice(written, written + itemsPerSecond)
        self.batchWrite(write, function(err) {
          if (err) return cb(err)
          written += write.length;
          cb();
        })
      })
    }

    async.whilst(function() {return written < items.length}, waitAndWrite, cb)
  })
}


proto.truncate = function(cb) {
  async.series([this.deleteTable.bind(this), this.createTable.bind(this)], cb)
}


proto.addNew = function(record, cb) {
  var key = Array.isArray(this.key) ? this.key[0] : this.key;

  if (record[key])
    return this.put(record, cb);

  var self = this;

  // this.nextId is added by `dynamo-table-id`, which must be loaded
  this.nextId(function(err, id) {
    if (err) return cb(err);
    record[key] = id;
    self.put(record, cb);
  });
}
