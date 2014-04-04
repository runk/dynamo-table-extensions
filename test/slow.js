var should = require('should')
var async = require('async')
var dynamoTableExtended = require('..')
var dynaliteServer = require('dynalite')()
var useLive = process.env.USE_LIVE_DYNAMO // set this (and AWS credentials) if you want to test on a live instance
var region = process.env.AWS_REGION // will just default to us-east-1 if not specified

describe('extensions', function() {

  var table

  before(function(done) {
    var port, setup = function(cb) { cb() }

    if (!useLive) {
      port = 10000 + Math.round(Math.random() * 10000)
      region = {host: 'localhost', port: port, credentials: {accessKeyId: 'a', secretAccessKey: 'a'}}
      setup = dynaliteServer.listen.bind(dynaliteServer, port)
    }

    table = dynamoTableExtended('dynamo-table-extension-test', {
      region: region,
      key: ['name'],
      mappings: {name: 'S'},
      writeCapacity: 2
    })

    setup(function(err) {
      if (err) return done(err)
      async.series([table.deleteTableAndWait.bind(table), table.createTableAndWait.bind(table)], done)
    })
  })

  after(function (done) {
    table.deleteTableAndWait(done)
  })

  beforeEach(function(done) {
    table.scan(function(err, items) {
      if (err || !items.length) return done(err)

      var ids = items.map(function(item) {
        return {forumName: item.forumName, subject: item.subject}
      })

      table.batchWrite({deletes: ids}, done)
    })
  })

  describe('throttledBatchWrite', function() {
    var originalBatchWrite
    var batchWriteCount
    var batchWriteError
    var writtenItems

    beforeEach(function() {
      originalBatchWrite = table.batchWrite
      table.batchWrite = function(items, cb) {
        ++batchWriteCount
        writtenItems = writtenItems.concat(items)
        cb(batchWriteError)
      }
      batchWriteCount = 0
      batchWriteError = null
      writtenItems = []
    })

    afterEach(function() {
      table.batchWrite = originalBatchWrite
    })

    it('should reject non-positive ratios', function(done) {
      table.throttledBatchWrite(NaN, [], function(err) {
        err.should.match(/positive/)
        should(batchWriteCount).equal(0)
        done()
      })
    })

    it('should write at half capacity without delay', function(done) {
      start = Date.now()
      table.throttledBatchWrite(0.5, [1], function(err) {
        should(Date.now() - start).be.below(1000)
        should(batchWriteCount).equal(1)
        writtenItems.should.eql([1])
        done()
      })
    })

    it('should write at full capacity without delay', function(done) {
      start = Date.now()
      table.throttledBatchWrite(1.0, [1, 2], function(err) {
        should(Date.now() - start).be.below(1000)
        should(batchWriteCount).equal(1)
        writtenItems.should.eql([1, 2])
        done()
      })
    })

    it('should write without exceeding capacity', function(done) {
      start = Date.now()
      table.throttledBatchWrite(1.0, [1, 2, 3, 4, 5], function(err) {
        should(Date.now() - start).be.within(2000, 2900)
        should(batchWriteCount).equal(3)
        writtenItems.should.eql([1, 2, 3, 4, 5])
        done()
      })
    })

    it('should properly handle batchWrite errors', function(done) {
      batchWriteError = new Error('test')
      table.throttledBatchWrite(1.0, [1, 2, 3, 4, 5], function(err) {
        err.should.match(/test/)
        done()
      })
    })
  })
})

