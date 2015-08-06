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
      key: ['id'],
      mappings: {id: 'N'},
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


  describe('truncate()', function() {

    before(function(done) {
      table.batchWrite([{id: 1, forumName: 'a', subject: 'b'}], done)
    })

    it('should remove everything from the table', function(done) {
      table.truncate(function(err) {
        if (err) return done(err)
        table.scan(function(err, info) {
          if (err) return done(err)
          info.should.eql([])
          done()
        })
      })
    })
  })


  describe('addNew()', function() {

    before(function(done) {
      table.batchWrite([{id: 1, forumName: 'a', subject: 'b'}], done)
      // `dynamo-table-id` mock
      table.nextId = function(cb) { cb(null, 2) }
    })

    it('should update existing record if `id` is defined', function(done) {
      var record = {id: 1, forumName: 'c', subject: 'd'}
      table.addNew(record, function(err) {
        if (err) return done(err)
        table.scan(function(err, records) {
          if (err) return done(err)
          records.should.eql([{id: 1, forumName: 'c', subject: 'd'}])
          done()
        })
      })
    })

    it('should create new record if `id` is undefined', function(done) {
      var record = {forumName: 'e', subject: 'f'}
      table.addNew(record, function(err) {
        if (err) return done(err)
        table.scan(function(err, records) {
          if (err) return done(err)
          // dynamo doesn't guarantee any ordering
          records.sort(function(a, b) { return a.id - b.id })
          records.should.eql([
            {id: 1, forumName: 'c', subject: 'd'},
            {id: 2, forumName: 'e', subject: 'f'}
          ])
          done()
        })
      })
    })
  })

})
