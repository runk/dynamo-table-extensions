var DynamoTable, async = require('async')

try {
  DynamoTable = require('dynamo-table').DynamoTable
} catch (e) {
  // Assume consumer will pass in dynamo table
}


// Ensures that no more than capacityRatio * writeCapacity items are written per second
DynamoTable.prototype.throttledBatchWrite = function(capacityRatio, items, cb) {
  if (!(capacityRatio > 0)) return cb(new Error('capacityRatio must be positive'))

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

