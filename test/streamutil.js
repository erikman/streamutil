import stream from 'stream';
import Promise from 'bluebird';

import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';
chai.use(chaiAsPromised);
let assert = chai.assert;

import * as streamutil from '../lib/streamutil';

describe('node streams', () => {
  describe('stream.Readable', () => {
    it('errors do not affect the stream', () => {
      class ReadableErrorStream extends stream.Readable {
        constructor() {
          super();
          this.step = 0;
        }

        _read() {
          // console.log('called _read', this.step);

          switch (this.step++) {
            case 0:
              this.push('first some data');
              break;

            case 1:
              process.nextTick(() => {
                this.emit('error', new Error('A test error!'));

                // _read is only invoked after 'push' has been invoked, so
                // continue the stream.
                this.push('data after error');
              });
              break;

            case 2:
              this.push('some more data');
              break;

            case 3:
              // do nothing, will not call _read again untill push has been
              // called
              break;

            default:
              assert.fail('Should never come here');
          }
        }
      }

      let errorStream = new ReadableErrorStream();
      errorStream.resume();
      return assert.isRejected(streamutil.waitForStream(errorStream), 'A test error!');
    });

    describe('eventlog', () => {
      // We run a simulation and record what is happening and then verify the
      // steps.
      let eventLog;

      beforeEach(() => {
        eventLog = [];
      });

      const EVENT_TYPE = {
        FLOW: 'FLOW',
        READ: 'READ',
        PUSH: 'PUSH',
        PUSH_BLOCKS: 'PUSH_BLOCKS',
        READ_END: 'READ_END',

        WRITE: 'WRITE',
        WRITEV: 'WRITEV',
        WRITE_NEXT: 'WRITE_NEXT',
        CONSUME: 'CONSUME',
        TIMEOUT: 'TIMEOUT'
      };

      let logDebug = () => {}; // console.log;

      class Source extends stream.Readable {
        constructor(options) {
          super({
            objectMode: true,
            highWaterMark: 4,
            ...options
          });

          this.maxCount = options.length;
          this.readCount = 0;
          this.isFlowing = false;

          this.flowCount = 0;
        }

        _read() {
          logDebug(`stream.Readable._read() readCount:${this.readCount} flowCount:${this.flowCount}`);
          eventLog.push({type: EVENT_TYPE.READ, readCount: this.readCount, flowCount: this.flowCount});

          this.isFlowing = true;
          while (this.readCount < this.maxCount && this.readCount < this.flowCount) {
            logDebug(`stream.Readable._read() - push(${this.readCount})`);
            eventLog.push({type: EVENT_TYPE.PUSH, readCount: this.readCount});
            if (!this.push(this.readCount++)) {
              // Rememeber that the push blocks at this offset
              logDebug(`stream.Readable._read() - push(${this.readCount - 1}) blocks`);
              eventLog.push({
                type: EVENT_TYPE.PUSH_BLOCKS,
                readCount: this.readCount - 1
              });
              return;
            }
          }

          if (this.readCount >= this.maxCount) {
            // End of stream
            eventLog.push({type: EVENT_TYPE.READ_END, readCount: this.readCount, flowCount: this.flowCount});

            logDebug('stream.Readable._read() end');
            this.push(null);
          }
        }

        flow(count) {
          logDebug(`stream.Readable.flow(${count}) readCount:${this.readCount}`);
          eventLog.push({
            type: EVENT_TYPE.FLOW,
            count,
            readCount: this.readCount,
            flowCount: this.flowCount
          });

          this.flowCount += count;
          if (this.isFlowing
              && (this.readCount < this.maxCount)
              && (this.readCount < this.flowCount)) {
            // Push one more element so _read gets triggered
            logDebug(`stream.Readable.flow() - push(${this.readCount})`);
            eventLog.push({type: EVENT_TYPE.PUSH, readCount: this.readCount});
            if (!this.push(this.readCount++)) {
              eventLog.push({type: EVENT_TYPE.PUSH_BLOCKS, readCount: this.readCount - 1});
              logDebug(`stream.Readable.flow() - push(${this.readCount - 1}) blocks`);
            }
          }
        }
      }

      class Sink extends stream.Writable {
        constructor(options) {
          super({
            objectMode: true,
            highWaterMark: 8,
            ...options
          });

          this.consumedCount = 0; // number of blocks consumed (or want to be consumed)
          this.writeCount = 0; // number of chunks received via _write or _writev

          this._next = null;
        }

        consume(count) {
          logDebug(`Sink.consume(${count}) writeCount:${this.writeCount}, consumedCount:${this.consumedCount} hasNext:${Boolean(this._next)}`);

          eventLog.push({
            type: EVENT_TYPE.CONSUME,
            count: count,
            writeCount: this.writeCount,
            consumedCount: this.consumedCount
          });

          this.consumedCount += count;

          let next = null;
          if (this._next && (this.consumedCount >= this.writeCount)) {
            next = this._next;
            this._next = null;
          }

          if (next) {
            logDebug('Sink.consume(${count}) next()');
            eventLog.push({
              type: EVENT_TYPE.WRITE_NEXT,
              writeCount: this.writeCount,
              consumedCount: this.consumedCount
            });

            next();
          }
        }

        _write(chunk, encoding, next) {
          eventLog.push({
            type: EVENT_TYPE.WRITE,
            writeCount: this.writeCount,
            consumedCount: this.consumedCount
          });

          logDebug(`stream.Writable._write(${chunk}) writeCount:${this.writeCount} consumedCount:${this.consumedCount}`);
          this.writeCount++;

          if (this.consumedCount >= this.writeCount) {
            logDebug(`stream.Writable._write(${chunk}) next()`);
            eventLog.push({
              type: EVENT_TYPE.WRITE_NEXT,
              writeCount: this.writeCount,
              consumedCount: this.consumedCount
            });
            next();
          }
          else {
            this._next = next;
          }
        }

        _writev(chunks, next) {
          eventLog.push({
            type: EVENT_TYPE.WRITEV,
            chunkCount: chunks.length,
            firstChunk: chunks[0].chunk,
            lastChunk: chunks[chunks.length - 1].chunk,
            consumedCount: this.consumedCount,
            writeCount: this.writeCount
          });

          logDebug(`stream.Writable._writev(${chunks[0].chunk} to ${chunks[chunks.length - 1].chunk}, chunks.length: ${chunks.length}) writeCount:${this.writeCount} consumedCount:${this.consumedCount}`);

          this.writeCount += chunks.length;

          if (this.consumedCount >= this.writeCount) {
            // We have more scheduled consumes than data. Consume everything
            // and ask for more
            logDebug(`stream.Writable._writev() next()`);
            eventLog.push({
              type: EVENT_TYPE.WRITE_NEXT,
              writeCount: this.writeCount,
              consumedCount: this.consumedCount
            });
            next();
          }
          else {
            // We have more data than scheduled consumes, need to wait for
            // consume to be called.
            this._next = next;
          }
        }
      }

      it('will use writev when source is faster than sink', done => {
        let expectedEventLog = [
          {type: 'FLOW', count: 10, readCount: 0, flowCount: 0},
          {type: 'READ', readCount: 0, flowCount: 10},
          {type: 'PUSH', readCount: 0},
          {type: 'PUSH', readCount: 1},
          {type: 'PUSH', readCount: 2},
          {type: 'PUSH', readCount: 3},
          {type: 'PUSH', readCount: 4},
          {type: 'PUSH_BLOCKS', readCount: 4}, // Source buffer is full
          {type: 'READ', readCount: 5, flowCount: 10}, // First entry in sink
          {type: 'PUSH', readCount: 5},
          {type: 'PUSH_BLOCKS', readCount: 5},
          {type: 'WRITE', writeCount: 0, consumedCount: 0}, // Write first entry to sink - no consume has been invoked so we block
          {type: 'READ', readCount: 6, flowCount: 10}, // Fill up more data in sink buffer
          {type: 'PUSH', readCount: 6},
          {type: 'PUSH_BLOCKS', readCount: 6},
          {type: 'READ', readCount: 7, flowCount: 10},
          {type: 'PUSH', readCount: 7},
          {type: 'PUSH_BLOCKS', readCount: 7},
          {type: 'TIMEOUT', block: 0},

          // First block, consume the entry we are processing
          {type: 'CONSUME', count: 1, writeCount: 1, consumedCount: 0},
          {type: 'WRITE_NEXT', writeCount: 1, consumedCount: 1},

          // Sink buffer is sent as a writev, don't consume yet so we block
          {type: 'WRITEV',
            chunkCount: 2,
            firstChunk: 1,
            lastChunk: 2,
            consumedCount: 1,
            writeCount: 1},

          // Second block, consume the rest
          {type: 'TIMEOUT', block: 1},
          {type: 'CONSUME', count: 9, writeCount: 3, consumedCount: 1},
          {type: 'WRITE_NEXT', writeCount: 3, consumedCount: 10},

          // We are fast so we will get each element individually
          {type: 'READ', readCount: 8, flowCount: 10},
          {type: 'PUSH', readCount: 8},
          {type: 'PUSH_BLOCKS', readCount: 8},
          {type: 'WRITE', writeCount: 3, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 4, consumedCount: 10},
          {type: 'READ', readCount: 9, flowCount: 10},
          {type: 'PUSH', readCount: 9},
          {type: 'PUSH_BLOCKS', readCount: 9},
          {type: 'WRITE', writeCount: 4, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 5, consumedCount: 10},
          {type: 'READ', readCount: 10, flowCount: 10},
          {type: 'READ_END', readCount: 10, flowCount: 10},
          {type: 'WRITE', writeCount: 5, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 6, consumedCount: 10},
          {type: 'WRITE', writeCount: 6, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 7, consumedCount: 10},
          {type: 'WRITE', writeCount: 7, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 8, consumedCount: 10},
          {type: 'WRITE', writeCount: 8, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 9, consumedCount: 10},
          {type: 'WRITE', writeCount: 9, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 10, consumedCount: 10}
        ];

        let source = new Source({highWaterMark: 5, length: 10});
        let sink = new Sink({highWaterMark: 3});

        source.flow(10);
        source.pipe(sink);

        // Wait for the buffers to fill up
        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 0
          });

          logDebug('timeout block:0');

          // Consume 1, will be taken from the buffered ones in the sink.
          // no read will be issued.
          sink.consume(1);
        }, 16);

        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 1
          });

          logDebug('timeout block:1');

          // Consume 1, will be taken from the buffered ones in the sink.
          // no read will be issued.
          sink.consume(9);
        }, 32);

        sink.on('finish', () => {
          logDebug('events', eventLog);

          assert.deepEqual(eventLog, expectedEventLog);
          done();
        });
      });

      it('it will allow for multiple #push events when there are empty elements in source buffer (source slower than sink)', done => {
        let expectedEventLog = [
          {type: 'CONSUME', count: 10, writeCount: 0, consumedCount: 0},
          {type: 'READ', readCount: 0, flowCount: 0}, // First read, we don't flow so block
          {type: 'TIMEOUT', block: 0},
          {type: 'FLOW', count: 1, readCount: 0, flowCount: 0}, // Done with reading the first element
          {type: 'PUSH', readCount: 0},
          {type: 'WRITE', writeCount: 0, consumedCount: 10}, // Is sent to sink
          {type: 'WRITE_NEXT', writeCount: 1, consumedCount: 10},
          {type: 'READ', readCount: 1, flowCount: 1}, // Read element, 2 we block
          {type: 'TIMEOUT', block: 1},
          {type: 'FLOW', count: 9, readCount: 1, flowCount: 1}, // Flow the reset
          {type: 'PUSH', readCount: 1},
          {type: 'WRITE', writeCount: 1, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 2, consumedCount: 10},
          {type: 'READ', readCount: 2, flowCount: 10},
          {type: 'PUSH', readCount: 2},
          {type: 'PUSH', readCount: 3},
          {type: 'PUSH', readCount: 4},
          {type: 'PUSH', readCount: 5},
          {type: 'PUSH', readCount: 6},
          {type: 'PUSH_BLOCKS', readCount: 6}, // Read buffer full
          {type: 'READ', readCount: 7, flowCount: 10}, // Send into write sink buffer
          {type: 'PUSH', readCount: 7},
          {type: 'PUSH_BLOCKS', readCount: 7},
          {type: 'WRITE', writeCount: 2, consumedCount: 10}, // Consume immediately
          {type: 'WRITE_NEXT', writeCount: 3, consumedCount: 10},
          {type: 'READ', readCount: 8, flowCount: 10},
          {type: 'PUSH', readCount: 8},
          {type: 'PUSH_BLOCKS', readCount: 8},
          {type: 'WRITE', writeCount: 3, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 4, consumedCount: 10},
          {type: 'READ', readCount: 9, flowCount: 10},
          {type: 'PUSH', readCount: 9},
          {type: 'PUSH_BLOCKS', readCount: 9},
          {type: 'WRITE', writeCount: 4, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 5, consumedCount: 10},
          {type: 'READ', readCount: 10, flowCount: 10},
          {type: 'READ_END', readCount: 10, flowCount: 10},
          {type: 'WRITE', writeCount: 5, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 6, consumedCount: 10},
          {type: 'WRITE', writeCount: 6, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 7, consumedCount: 10},
          {type: 'WRITE', writeCount: 7, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 8, consumedCount: 10},
          {type: 'WRITE', writeCount: 8, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 9, consumedCount: 10},
          {type: 'WRITE', writeCount: 9, consumedCount: 10},
          {type: 'WRITE_NEXT', writeCount: 10, consumedCount: 10}
        ];

        let source = new Source({highWaterMark: 5, length: 10});
        let sink = new Sink({highWaterMark: 3});

        sink.consume(10);
        source.pipe(sink);

        // Wait for the buffers to fill up
        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 0
          });

          logDebug('timeout block:0');

          source.flow(1);
        }, 16);

        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 1
          });

          logDebug('timeout block:1');

          source.flow(9);
        }, 32);

        sink.on('finish', () => {
          logDebug('events', eventLog);

          assert.deepEqual(eventLog, expectedEventLog);
          done();
        });
      });

      it('will call _read when there are free entries in the internal buffer', done => {
        let expectedEventLog = [
          {type: 'FLOW', count: 36, readCount: 0, flowCount: 0},
          {type: 'READ', readCount: 0, flowCount: 36},
          {type: 'PUSH', readCount: 0},
          {type: 'PUSH', readCount: 1},
          {type: 'PUSH', readCount: 2},
          {type: 'PUSH', readCount: 3},
          {type: 'PUSH_BLOCKS', readCount: 3},
          {type: 'READ', readCount: 4, flowCount: 36},
          {type: 'PUSH', readCount: 4},
          {type: 'PUSH_BLOCKS', readCount: 4},
          {type: 'WRITE', writeCount: 0, consumedCount: 0},
          {type: 'READ', readCount: 5, flowCount: 36},
          {type: 'PUSH', readCount: 5},
          {type: 'PUSH_BLOCKS', readCount: 5},
          {type: 'READ', readCount: 6, flowCount: 36},
          {type: 'PUSH', readCount: 6},
          {type: 'PUSH_BLOCKS', readCount: 6},
          {type: 'READ', readCount: 7, flowCount: 36},
          {type: 'PUSH', readCount: 7},
          {type: 'PUSH_BLOCKS', readCount: 7},
          {type: 'READ', readCount: 8, flowCount: 36},
          {type: 'PUSH', readCount: 8},
          {type: 'PUSH_BLOCKS', readCount: 8},
          {type: 'READ', readCount: 9, flowCount: 36},
          {type: 'PUSH', readCount: 9},
          {type: 'PUSH_BLOCKS', readCount: 9},
          {type: 'READ', readCount: 10, flowCount: 36},
          {type: 'PUSH', readCount: 10},
          {type: 'PUSH_BLOCKS', readCount: 10},
          {type: 'READ', readCount: 11, flowCount: 36},
          {type: 'PUSH', readCount: 11},
          {type: 'PUSH_BLOCKS', readCount: 11},
          {type: 'TIMEOUT', block: 0},
          {type: 'CONSUME', count: 1, writeCount: 1, consumedCount: 0},
          {type: 'WRITE_NEXT', writeCount: 1, consumedCount: 1},
          {type: 'WRITEV',
            chunkCount: 7,
            firstChunk: 1,
            lastChunk: 7,
            consumedCount: 1,
            writeCount: 1},
          {type: 'TIMEOUT', block: 1},
          {type: 'CONSUME', count: 7, writeCount: 8, consumedCount: 1},
          {type: 'WRITE_NEXT', writeCount: 8, consumedCount: 8},
          {type: 'READ', readCount: 12, flowCount: 36},
          {type: 'PUSH', readCount: 12},
          {type: 'PUSH_BLOCKS', readCount: 12},
          {type: 'WRITE', writeCount: 8, consumedCount: 8},
          {type: 'READ', readCount: 13, flowCount: 36},
          {type: 'PUSH', readCount: 13},
          {type: 'PUSH_BLOCKS', readCount: 13},
          {type: 'READ', readCount: 14, flowCount: 36},
          {type: 'PUSH', readCount: 14},
          {type: 'PUSH_BLOCKS', readCount: 14},
          {type: 'READ', readCount: 15, flowCount: 36},
          {type: 'PUSH', readCount: 15},
          {type: 'PUSH_BLOCKS', readCount: 15},
          {type: 'READ', readCount: 16, flowCount: 36},
          {type: 'PUSH', readCount: 16},
          {type: 'PUSH_BLOCKS', readCount: 16},
          {type: 'READ', readCount: 17, flowCount: 36},
          {type: 'PUSH', readCount: 17},
          {type: 'PUSH_BLOCKS', readCount: 17},
          {type: 'READ', readCount: 18, flowCount: 36},
          {type: 'PUSH', readCount: 18},
          {type: 'PUSH_BLOCKS', readCount: 18},
          {type: 'READ', readCount: 19, flowCount: 36},
          {type: 'PUSH', readCount: 19},
          {type: 'PUSH_BLOCKS', readCount: 19},
          {type: 'TIMEOUT', block: 2},
          {type: 'CONSUME', count: 12, writeCount: 9, consumedCount: 8},
          {type: 'WRITE_NEXT', writeCount: 9, consumedCount: 20},
          {type: 'WRITEV',
            chunkCount: 7,
            firstChunk: 9,
            lastChunk: 15,
            consumedCount: 20,
            writeCount: 9},
          {type: 'WRITE_NEXT', writeCount: 16, consumedCount: 20},
          {type: 'READ', readCount: 20, flowCount: 36},
          {type: 'PUSH', readCount: 20},
          {type: 'PUSH_BLOCKS', readCount: 20},
          {type: 'WRITE', writeCount: 16, consumedCount: 20},
          {type: 'WRITE_NEXT', writeCount: 17, consumedCount: 20},
          {type: 'READ', readCount: 21, flowCount: 36},
          {type: 'PUSH', readCount: 21},
          {type: 'PUSH_BLOCKS', readCount: 21},
          {type: 'WRITE', writeCount: 17, consumedCount: 20},
          {type: 'WRITE_NEXT', writeCount: 18, consumedCount: 20},
          {type: 'READ', readCount: 22, flowCount: 36},
          {type: 'PUSH', readCount: 22},
          {type: 'PUSH_BLOCKS', readCount: 22},
          {type: 'WRITE', writeCount: 18, consumedCount: 20},
          {type: 'WRITE_NEXT', writeCount: 19, consumedCount: 20},
          {type: 'READ', readCount: 23, flowCount: 36},
          {type: 'PUSH', readCount: 23},
          {type: 'PUSH_BLOCKS', readCount: 23},
          {type: 'WRITE', writeCount: 19, consumedCount: 20},
          {type: 'WRITE_NEXT', writeCount: 20, consumedCount: 20},
          {type: 'READ', readCount: 24, flowCount: 36},
          {type: 'PUSH', readCount: 24},
          {type: 'PUSH_BLOCKS', readCount: 24},
          {type: 'WRITE', writeCount: 20, consumedCount: 20},
          {type: 'READ', readCount: 25, flowCount: 36},
          {type: 'PUSH', readCount: 25},
          {type: 'PUSH_BLOCKS', readCount: 25},
          {type: 'READ', readCount: 26, flowCount: 36},
          {type: 'PUSH', readCount: 26},
          {type: 'PUSH_BLOCKS', readCount: 26},
          {type: 'READ', readCount: 27, flowCount: 36},
          {type: 'PUSH', readCount: 27},
          {type: 'PUSH_BLOCKS', readCount: 27},
          {type: 'READ', readCount: 28, flowCount: 36},
          {type: 'PUSH', readCount: 28},
          {type: 'PUSH_BLOCKS', readCount: 28},
          {type: 'READ', readCount: 29, flowCount: 36},
          {type: 'PUSH', readCount: 29},
          {type: 'PUSH_BLOCKS', readCount: 29},
          {type: 'READ', readCount: 30, flowCount: 36},
          {type: 'PUSH', readCount: 30},
          {type: 'PUSH_BLOCKS', readCount: 30},
          {type: 'READ', readCount: 31, flowCount: 36},
          {type: 'PUSH', readCount: 31},
          {type: 'PUSH_BLOCKS', readCount: 31},
          {type: 'TIMEOUT', block: 3},
          {type: 'CONSUME', count: 16, writeCount: 21, consumedCount: 20},
          {type: 'WRITE_NEXT', writeCount: 21, consumedCount: 36},
          {type: 'WRITEV',
            chunkCount: 7,
            firstChunk: 21,
            lastChunk: 27,
            consumedCount: 36,
            writeCount: 21},
          {type: 'WRITE_NEXT', writeCount: 28, consumedCount: 36},
          {type: 'READ', readCount: 32, flowCount: 36},
          {type: 'PUSH', readCount: 32},
          {type: 'PUSH_BLOCKS', readCount: 32},
          {type: 'WRITE', writeCount: 28, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 29, consumedCount: 36},
          {type: 'READ', readCount: 33, flowCount: 36},
          {type: 'PUSH', readCount: 33},
          {type: 'PUSH_BLOCKS', readCount: 33},
          {type: 'WRITE', writeCount: 29, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 30, consumedCount: 36},
          {type: 'READ', readCount: 34, flowCount: 36},
          {type: 'PUSH', readCount: 34},
          {type: 'PUSH_BLOCKS', readCount: 34},
          {type: 'WRITE', writeCount: 30, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 31, consumedCount: 36},
          {type: 'READ', readCount: 35, flowCount: 36},
          {type: 'PUSH', readCount: 35},
          {type: 'PUSH_BLOCKS', readCount: 35},
          {type: 'WRITE', writeCount: 31, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 32, consumedCount: 36},
          {type: 'READ', readCount: 36, flowCount: 36},
          {type: 'READ_END', readCount: 36, flowCount: 36},
          {type: 'WRITE', writeCount: 32, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 33, consumedCount: 36},
          {type: 'WRITE', writeCount: 33, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 34, consumedCount: 36},
          {type: 'WRITE', writeCount: 34, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 35, consumedCount: 36},
          {type: 'WRITE', writeCount: 35, consumedCount: 36},
          {type: 'WRITE_NEXT', writeCount: 36, consumedCount: 36}
        ];

        let source = new Source({length: 36});
        let sink = new Sink();

        source.flow(36);
        source.pipe(sink);

        // Wait for the buffers to fill up
        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 0
          });

          logDebug('timeout1');

          // Consume 1, will be taken from the buffered ones in the sink.
          // no read will be issued.
          sink.consume(1);
        }, 10);

        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 1
          });

          logDebug('timeout2');

          // Consume the rest of the events in the sink buffer
          sink.consume(7);

          // The last sink will drain the sink buffer and new reads will be
          // issued to fill the sink buffer.
        }, 32);

        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 2
          });

          logDebug('timeout3');
          sink.consume(12);
        }, 64);

        setTimeout(() => {
          eventLog.push({
            type: EVENT_TYPE.TIMEOUT,
            block: 3
          });

          logDebug('timeout4');
          sink.consume(16);
        }, 96);

        sink.on('finish', () => {
          logDebug('events', eventLog);

          assert.deepEqual(eventLog, expectedEventLog);
          done();
        });
      });
    });
  });
});

describe('streamutil', () => {
  describe('ArrayReader', () => {
    it('should handle empty arrays', () => {
      let testArray = [];
      let stream = new streamutil.ArrayReader(testArray);
      assert.equal(stream.read(), null);
    });

    it('should be possible to read the elements from an array', () => {
      let testArray = [2, 3, 4, 5];
      let stream = new streamutil.ArrayReader(testArray);
      assert.equal(stream.read(), 2);
      assert.equal(stream.read(), 3);
      assert.equal(stream.read(), 4);
      assert.equal(stream.read(), 5);

      assert.equal(stream.read(), null);
    });
  });

  describe('arrayToStream', () => {
    it('should handle empty arrays', () => {
      let testArray = [];
      let stream = streamutil.arrayToStream(testArray);
      assert.equal(stream.read(), null);
    });

    it('should be possible to read the elements from an array', () => {
      let testArray = [2, 3, 4, 5];
      let stream = streamutil.arrayToStream(testArray);
      assert.equal(stream.read(), 2);
      assert.equal(stream.read(), 3);
      assert.equal(stream.read(), 4);
      assert.equal(stream.read(), 5);

      assert.equal(stream.read(), null);
    });
  });

  describe('ArrayWriter', () => {
    it('should be possible to write elements to an array', () => {
      let stream = new streamutil.ArrayWriter();
      stream.write(2);
      stream.write(3);

      assert.equal(stream.data[0], 2);
      assert.equal(stream.data[1], 3);
    });

    it('should be possible to pipe from input to output', done => {
      let data = [2, 3, 4, 5];
      let readArrayStream = new streamutil.ArrayReader(data);
      let writeArrayStream = new streamutil.ArrayWriter();

      readArrayStream.pipe(writeArrayStream).on('finish', () => {
        assert.equal(writeArrayStream.data.length, data.length);
        done();
      });
    });
  });

  describe('streamToArray', () => {
    it('can return an array from a stream', () => {
      return assert.eventually.deepEqual(
        streamutil.streamToArray(streamutil.arrayToStream([2, 3, 4, 5])),
        [2, 3, 4, 5]);
    });
  });

  describe('LastInStream', () => {
    it('should return last from a stream of objects', done => {
      let testArray = [2, 3, 4, 5];
      let stream = new streamutil.ArrayReader(testArray)
        .pipe(new streamutil.LastInStream());

      stream.on('finish', () => {
        assert.equal(stream.last, 5);
        done();
      });
    });

    it('should return null on empty streams', () => {
      let testArray = [];
      let stream = new streamutil.ArrayReader(testArray)
        .pipe(new streamutil.LastInStream());

      return streamutil.waitForStream(stream).then(() => {
        assert.equal(stream.last, null);
      });
    });
  });

  describe('last', () => {
    it('should return last from a stream of objects', () => {
      let testArray = [2, 3, 4, 5];
      let stream = new streamutil.ArrayReader(testArray);

      return assert.eventually.equal(streamutil.last(stream), 5);
    });

    it('should return defaultValue on empty streams', () => {
      let testArray = [];
      let stream = new streamutil.ArrayReader(testArray);

      return assert.eventually.equal(streamutil.last(stream, 'default value'), 'default value');
    });

    it('should return undefined on empty stream without defaultValue', () => {
      let testArray = [];
      let stream = new streamutil.ArrayReader(testArray);

      return assert.eventually.isUndefined(streamutil.last(stream));
    });
  });

  describe('transform', () => {
    it('can transform elements in a stream', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.transform((push, x) => {
          push(x + 1);
          push(x * x);
        })
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [3, 4, 4, 9, 5, 16, 6, 25]);
    });

    it('can have promises as return values', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.transform((push, x) => Promise.delay(20).then(() => {
          push(x + 1);
          push(x * x);
        }))
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [3, 4, 4, 9, 5, 16, 6, 25]);
    });
  });

  describe('map', () => {
    it('can transform elements in a stream', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.map(x => x * x)
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [4, 9, 16, 25]);
    });

    it('can have promises as return values', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.map(x => Promise.delay(20).then(() => x * x))
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [4, 9, 16, 25]);
    });
  });

  describe('filter', () => {
    it('can remove elements from a stream', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.filter(x => x % 2 == 0)
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [2, 4]);
    });

    it('can have promises as return values', () => {
      let mapStream = streamutil.pipeline([
        streamutil.arrayToStream([2, 3, 4, 5]),
        streamutil.filter(x => Promise.delay(20).then(() => x % 2 == 0))
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(mapStream),
        [2, 4]);
    });
  });

  describe('scanStream', () => {
    it('can calculate a cumulative sum', () => {
      let testArray = [1, 2, 3, 4, 5];
      let stream = new streamutil.ArrayReader(testArray)
        .pipe(streamutil.scanStream((x, y) => x + y, 0))
        .pipe(new streamutil.ArrayWriter());

      return streamutil.waitForStream(stream).then(() => {
        assert.deepEqual(stream.data, [1, 3, 6, 10, 15]);
      });
    });
  });

  describe('pipeline', () => {
    it('rejects no streams', () => {
      assert.throws(streamutil.pipeline);
    });

    it('handles a single stream', () => {
      let readableStream = new stream.Readable({
        read() {
          this.push('lalla');
          this.push(null);
        }
      });

      let pipelinedStream = streamutil.pipeline([readableStream]);
      return assert.becomes(streamutil.joinChunks(pipelinedStream, 'utf-8'),
                            'lalla');
    });

    it('can connect streams', () => {
      let testArray = [1, 2, 3, 4, 5];

      let resultWriter = new streamutil.ArrayWriter();

      let stream = streamutil.pipeline([
        new streamutil.ArrayReader(testArray),
        streamutil.scanStream((x, y) => x + y, 0),
        resultWriter
      ]);

      return streamutil.waitForStream(stream).then(() => {
        assert.deepEqual(resultWriter.data, [1, 3, 6, 10, 15]);
      });
    });

    // TODO: Handles errors
    // TODO: Handles internal close events
  });

  describe('literal', () => {
    it('returns the data given', () => {
      let stream = streamutil.literal('This is a test string');

      let data = null;
      stream.on('data', d => {
        try {
          assert(data === null);
          data = d;
        }
        catch (err) {
          stream.emit('error', err);
        }
      });

      return streamutil.waitForStream(stream).then(() => {
        assert.equal(data, 'This is a test string');
      });
    });

    it('can return data from a proimse', () => {
      let stream = streamutil.literal(Promise.resolve('This is a test string'));

      let data = null;
      stream.on('data', d => {
        try {
          assert(data === null);
          data = d;
        }
        catch (err) {
          stream.emit('error', err);
        }
      });

      return streamutil.waitForStream(stream).then(() => {
        assert.equal(data, 'This is a test string');
      });
    });
  });

  describe('joinChunksStream', () => {
    it('returns the data given as a stream', () => {
      let resultWriter = streamutil.joinChunksStream();

      let stream = streamutil.pipeline([
        streamutil.concat(
          streamutil.literal('This is a '),
          streamutil.literal('test string')
        ),
        resultWriter
      ]);

      return streamutil.waitForStream(stream).then(() => {
        assert(resultWriter.data); // Should be a buffer
        assert.equal(resultWriter.data.toString(), 'This is a test string');
      });
    });
  });

  describe('joinChunks', () => {
    it('returns the data given as a promise', () => {
      let stream = streamutil.concat(
        streamutil.literal('This is a '),
        streamutil.literal('test string')
      );

      return streamutil.joinChunks(stream).then(combinedChunks => {
        assert.equal(combinedChunks.toString(), 'This is a test string');
      });
    });
  });

  describe('toJSON', () => {
    it('can convert a stream to a JSON array', () => {
      let testArray = [1, 2, 3, 4, 5];

      let stream = streamutil.pipeline([
        new streamutil.ArrayReader(testArray),
        streamutil.toJSON()
      ]);

      assert.becomes(streamutil.joinChunks(stream, 'utf-8'),
                     JSON.stringify(testArray));
    });
  });

  describe('waitForStream', () => {
    it('returns success when readable stream emits end', () => {
      let readableStream = streamutil.literal('Some data');
      readableStream.resume();

      return streamutil.waitForStream(readableStream);
    });

    it('returns success when writable stream emits finish', () => {
      let writableStream = streamutil.joinChunksStream();

      process.nextTick(() => {
        writableStream.write('lalal');
        writableStream.end();
      });

      return streamutil.waitForStream(writableStream);
    });

    it('rejects the promise when a stream emits an error', () => {
      let readableStream = streamutil.literal('Some data');

      process.nextTick(() => {
        readableStream.emit('error', new Error('Error state'));
      });

      return assert.isRejected(streamutil.waitForStream(readableStream));
    });

    it('rejects the promise when a stream closes', () => {
      let readableStream = streamutil.literal('Some data');

      process.nextTick(() => {
        readableStream.emit('close');
      });

      return assert.isRejected(streamutil.waitForStream(readableStream));
    });
  });

  describe('unwrapArrayStream', () => {
    it('unwraps elements from arrays', () => {
      let pipelineStream = streamutil.pipeline([
        streamutil.arrayToStream([
          [2, 3],
          [5, 6],
          [7, 8, 9]
        ]),
        streamutil.unwrapArrayStream()
      ]);

      return assert.eventually.deepEqual(
        streamutil.streamToArray(pipelineStream),
        [2, 3, 5, 6, 7, 8, 9]
      );
    });
  });
});
