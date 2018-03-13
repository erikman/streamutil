import stream from 'stream';
import events from 'events';
import assert from 'assert';
import _ from 'lodash';

function isReadableStream(possiblyReadableStream) {
  return _.isObject(possiblyReadableStream)
    && _.isFunction(possiblyReadableStream.read);
}

function isWritableStream(possiblyWritableStream) {
  return _.isObject(possiblyWritableStream)
    && _.isFunction(possiblyWritableStream.write);
}


class ReadableWrapper extends stream.Readable {
  constructor(options) {
    // Copy the 'objectMode' parameter from the wrapped stream.
    options = {
      objectMode: options.readStream._readableState.objectMode,
      ...options
    };

    super(options);
    this.readStream = options.readStream;

    if (!isReadableStream(this.readStream)) {
      throw new Error('Invalid read stream');
    }

    this.reading = false;
    this.readStream.on('end', () => this.push(null));
  }

  _read() {
    try {
      if (this.reading) {
        this.readStream.resume();
      }
      else {
        // Setup callback for passing data to our consumers
        this.readStream.on('data', chunk => {
          if (!this.push(chunk)) {
            this.readStream.pause();
          }
        });
        this.reading = true;
      }
    }
    catch (err) {
      this.emit('error', err);
    }
  }
}


class WritableWrapper extends stream.Writable {
  constructor(options) {
     // Copy the 'objectMode' parameter from the wrapped stream.
    options = {
      objectMode: options.writeStream._writableState.objectMode,
      ...options
    };

    super(options);
    this.writeStream = options.writeStream;

    if (!isWritableStream((this.writeStream))) {
      throw new Error('Invalid write stream');
    }

    this.writeStream.on('finish', () => this.emit('finish'));
  }

  _write(chunk, encoding, callback) {
    return this.writeStream.write(chunk, encoding, callback);
  }
}


class DuplexWrapper extends stream.Duplex {
  constructor(options) {
    // Copy the 'objectMode' parameter from the wrapped stream.
    options = {
      objectMode: options.readStream._readableState.objectMode
        || options.writeStream._writableState.objectMode,
      ...options
    };

    super(options);
    this.readStream = options.readStream;
    this.writeStream = options.writeStream;

    if (!isReadableStream(this.readStream)) {
      throw new Error('Invalid read stream');
    }
    if (!isWritableStream((this.writeStream))) {
      throw new Error('Invalid write stream');
    }

    this.reading = false;

    this.writeStream.on('finish', () => this.emit('finish'));
    this.readStream.on('end', () => this.push(null));

    // this.readStream.on('error', err => this.emit('error', err));
    // this.writeStream.on('error', err => this.emit('error', err));
  }

  _read() {
    if (!this.reading) {
      // Setup callback for passing data to our consumers
      this.readStream.on('data', chunk => {
        if (!this.push(chunk)) {
          this.readStream.pause();
        }
      });
      this.reading = true;
    }

    this.readStream.resume();
  }

  _write(chunk, encoding, callback) {
    return this.writeStream.write(chunk, encoding, callback);
  }
}


export class ArrayReader extends stream.Readable {
  constructor(options, data) {
    if (arguments.length === 1) {
      data = options;
      options = undefined;
    }

    super({
      ...options,
      objectMode: true
    });

    this.data = data;
    this.index = 0;
  }

  _read() {
    do {
      if (this.index >= this.data.length) {
        // End of stream
        this.push(null);
        return;
      }
    } while (this.push(this.data[this.index++]));
  }
}


export class ArrayWriter extends stream.Writable {
  constructor(options) {
    super({
      ...options,
      objectMode: true
    });

    this.data = [];
  }

  _write(object, encoding, done) {
    this.data.push(object);
    done();
  }
}


export class StreamPrinter extends stream.Writable {
  constructor(options) {
    super({
      ...options,
      objectMode: true
    });
  }

  _write(object, encoding, done) {
    console.log(object);
    done();
  }
}


export class StreamToJSON extends stream.Transform {
  constructor(options) {
    super({
      ...options,
      objectMode: true
    });

    this.state = {
      first: true
    };
  }

  _transform(object, encoding, done) {
    let jsonString = JSON.stringify(object);
    if (jsonString.length > 0) {
      if (this.state.first) {
        jsonString = '[' + jsonString;
        this.state.first = false;
      }
      else {
        jsonString = ',' + jsonString;
      }
      this.push(jsonString);
    }

    done();
  }

  _flush(done) {
    if (this.state.first) {
      this.push('[]');
    }
    else {
      this.push(']');
    }
    done();
  }
}


export class StreamToCSV extends stream.Transform {
  constructor(options) {
    super({
      ...options,
      objectMode: true
    });

    this.state = {
      first: true
    };
  }

  _transform(object, encoding, done) {
    if (this.state.first) {
      this.state.keys = Object.keys(object);
      this.state.first = false;
    }
    else {
      this.push(',');
    }

    const csvRow = this.state.keys.map(key => {
      return String(object[key]);
    }).join(';');

    this.push(csvRow + '\n');
    done();
  }
}


export class LastInStream extends stream.Writable {
  constructor(defaultValue, options) {
    super({
      ...options,
      objectMode: true
    });

    this.last = defaultValue;
  }

  _write(object, encoding, done) {
    this.last = _.cloneDeep(object);
    done();
  }
}


class TransformStream extends stream.Transform {
  constructor(transformer, options) {
    super({
      ...options,
      objectMode: true
    });
    this._transformer = transformer;
  }

  _transform(data, encoding, done) {
    try {
      const pusher = data => {
        this.push(data);
      };

      const transformResult = this._transformer(pusher, data, encoding);
      if (transformResult && _.isObject(transformResult) && _.isFunction(transformResult.then)) {
        // transformResult is a then-able Promise
        transformResult.then(() => {
          done();
        }).then(null, err => {
          done(err);
        });
      }
      else {
        done();
      }
    }
    catch (err) {
      process.nextTick(() => done(err));
    }
  }
}


class ScanStream extends stream.Transform {
  constructor(scanner, initialState, options) {
    super({
      ...options,
      objectMode: true
    });
    this._scanner = scanner;
    this.state = initialState;
  }

  _transform(data, encoding, done) {
    try {
      this.state = this._scanner(this.state, data);
      this.push(this.state);
      done();
    }
    catch (err) {
      process.nextTick(() => done(err));
    }
  }
}


// Concatenate streams
export class StreamStream extends stream.Readable {
  constructor(streams, options) {
    super({
      ...options,
      objectMode: true
    });

    if (!_.isArray(streams)) {
      throw new Error('Streams is not an array');
    }

    streams.forEach(pipelineStream => {
      if (!_.isObject(pipelineStream)) {
        throw new Error('stream must be an object');
      }
      if (!isReadableStream(pipelineStream)) {
        throw new Error('all streams must be readable');
      }
    });

    this.streams = streams;
    this.isNewStream = true;
  }

  _read() {
    while (this.streams.length > 0) {
      let pipelineStream = this.streams[0];
      if (this.isNewStream) {
        // Forward errors from underlying stream
        pipelineStream.on('error', err => this.emit('error', err));
        pipelineStream.on('data', chunk => {
          if (!this.push(chunk)) {
            // Stop reading from the source stream until read is called again
            pipelineStream.pause();
          }
        });
        pipelineStream.on('end', () => {
          // End of stream
          this.streams.shift();
          this.isNewStream = true;

          this._read();
        });
        this.isNewStream = false;
      }
      else {
        pipelineStream.resume();
        return;
      }
    }

    if (this.streams.length === 0) {
      // End of stream
      this.push(null);
      return;
    }
  }
}


// Similary to ArrayStream but only for one object.
export class LiteralStream  extends stream.Readable {
  constructor(data, options) {
    super({
      ...options,
      objectMode: !_.isString(data)
        && !(_.isObject(data) && (data instanceof Buffer))
    });

    this.hasEmittedData = false;
    this.data = data;
  }

  _read() {
    if (this.hasEmittedData) {
      this.push(null);
      return;
    }

    // Is the data a promise? If so wait for the proimse to resolve before
    // sending the data on.
    if (_.isObject(this.data) && _.isFunction(this.data.then)) {
      this.data.then(realData => {
        // Note: important to set the flag before push, as a recursive call
        // into _read might be issued.
        this.hasEmittedData = true;
        this.push(realData);
      }).catch(err => {
        process.nextTick(() => this.emit('error', err));
      });
    }
    else {
      this.push(this.data);
      this.hasEmittedData = true;
    }
  }
}


// Concatenate all stream output into one buffer
export class StreamToString extends stream.Writable {
  constructor(options) {
    super(options);
    this.data = null;
  }

  _write(chunk, encoding, done) {
    try {
      if (this.data) {
        this.data = Buffer.concat([this.data, chunk]);
      }
      else {
        // New syntax requires node > 5.1
        this.data = new Buffer(chunk); //  Buffer.from(chunk);
      }
      done();
    }
    catch (err) {
      done(err);
    }
  }
}


/**
 * Convert arrays to individual elements
 */
class UnwrapArrayStream extends stream.Transform {
  constructor(options) {
    super({
      ...options,
      objectMode: true
    });
  }

  _transform(chunk, encoding, done) {
    if (_.isArray(chunk)) {
      for (let i = 0; i < chunk.length; i++) {
        this.push(chunk[i]);
      }
      done();
    }
    else {
      this.push(chunk);
      done();
    }
  }
}

export function arrayToStream(xs) {
  return new ArrayReader(xs);
}


export function streamToArray(inStream) {
  let arrayWriter = new ArrayWriter();
  let pipelineStream = pipeline([inStream, arrayWriter]);
  return waitForStream(pipelineStream).then(() => arrayWriter.data);
}


export function unwrapArrayStream() {
  return new UnwrapArrayStream();
}


/**
 * Create a readable stream that returns a single value.
 */
export function literal(data) {
  return new LiteralStream(data);
}


/**
 * Join a set of readable streams after each other into one stream.
 */
export function concat(...streams) {
  return new StreamStream(streams);
}


export function toJSON() {
  return new StreamToJSON();
}


export function joinChunksStream() {
  return new StreamToString();
}

export function joinChunks(stream, encoding) {
  assert(stream, 'Stream must be defined');

  let outputStream = joinChunksStream();
  let connectedStreams = pipeline([
    stream,
    outputStream
  ]);
  return waitForStream(connectedStreams).then(() => {
    return encoding ? outputStream.data.toString(encoding) : outputStream.data;
  });
}


export function toCsv() {
  return new StreamToCSV();
}


export function transform(transformer) {
  return new TransformStream(transformer);
}


// Call 'mapper' function on each element (possibly returning promises).
export function map(mapper) {
  return new TransformStream((push, data) => {
    const mapResult = mapper(data);
    if (_.isObject(mapResult) && _.isFunction(mapResult.then)) {
      // mapResult is a then-able Promise
      return mapResult.then(res => {
        push(res);
      });
    }
    push(mapResult);
  });
}


export function filter(filtering) {
  return new TransformStream((push, data) => {
    const filterResult = filtering(data);
    if (_.isObject(filterResult) && _.isFunction(filterResult.then)) {
      // mapResult is a then-able Promise
      return filterResult.then(res => {
        if (res) {
          push(data);
        }
      });
    }
    else if (filterResult) {
      push(data);
    }
  });
}


export function scanStream(scanner, initialState, options) {
  return new ScanStream(scanner, initialState, options);
}


/**
 * Wait for a stream to finish or signal an error
 *
 * @return a promise that will resolve when the stream is done
 */
export function waitForStream(stream) {
  if (!(stream instanceof events.EventEmitter)) {
    throw new Error('stream is not an event emitter');
  }

  return new Promise((resolve, reject) => {
    // No more events will be emitted after a close.
    stream.on('close', reject);

    // End signal for writable streams
    stream.on('finish', resolve);

    // End signal for readable streams
    stream.on('end', resolve);

    stream.on('error', reject);
  });
}


/** Helper for debugging the signals on a stream */
export function printSignals(stream, ...debugArgs) {
  let signals = [
    'finish',
    'end',
    'error',
    'close',
    'drain',
    'pipe',
    'unpipe',
    'readable',
    'data'
  ];

  signals.forEach(signal => {
    stream.on(signal, (...signalArgs) => {
      console.log(...debugArgs, signal, ...signalArgs);
    });
  });
}


/**
 * Get the last object from an event stream.
 *
 * Only works for object streams.
 */
export function last(stream, defaultValue) {
  let lastStream = new LastInStream(defaultValue);

  return waitForStream(pipeline([stream, lastStream]))
    .then(() => lastStream.last);
}


// Connect streams together and make sure we propagate errors and handle when
// piped streams close unexpectedly.
export function pipeline(streams, errorCallback) {
  let resultStream = null;

  // When any stream closes/reports an error we want to close all of these
  // callbacks. The callbacks may be invoked multiple times, the callback must
  // ensure this does not lead to infinite recursion.
  let destroyCallbacks = [];
  function destroyStreams() {
    destroyCallbacks.forEach(destroyCallback => {
      try {
        destroyCallback();
      }
      catch (err) {
        errorHandler(err);
      }
    });
  }

  // Error handler for all the streams
  let savedError = null;
  function errorHandler(err) {
    if (savedError) {
      // We have already handled errors (or are in the process of handling
      // them).
      return;
    }
    savedError = err;

    // Destroy the streams
    destroyStreams();

    // Call the global error handler
    if (resultStream) {
      resultStream.emit('error', savedError);
    }
    if (errorCallback) {
      errorCallback(savedError);
    }
  }

  // Setup close/error callbacks for the streams
  streams.forEach((stream, index) => {
    if ((index > 0) && !isWritableStream(stream)) {
      throw new Error('Stream inside pipeline must be writeable');
    }
    if ((index < streams.length - 1) && !isReadableStream(stream)) {
      throw new Error('Stream inside pipeline must be readable');
    }
    stream.on('error', errorHandler);

    // Track ended/finished status
    let ended = !isReadableStream(stream);
    let finished = !isWritableStream(stream);
    stream.on('end', () => {
      ended = true;
    });
    stream.on('finished', () => {
      finished = true;
    });

    let closed = false;
    stream.on('close', () => {
      closed = true;
      if (!ended || !finished) {
        errorHandler(new Error('Stream inside pipe was closed prematurely'));
      }
    });

    let destroyed = false;
    destroyCallbacks.push(() => {
      if (closed || destroyed) {
        return;
      }
      destroyed = true;

      if (!closed && _.isFunction(stream.close)) {
        stream.close();
      }
      if (_.isFunction(stream.abort)) {
        stream.abort();
      }
      if (_.isFunction(stream.destroy)) {
        stream.destroy();
      }
    });
  });

  // Connect the streams together in a pipe
  streams.reduce((first, second) => {
    first.pipe(second);
    return second;
  });

  let firstStream = streams[0];
  let lastStream = streams[streams.length - 1];

  if (isWritableStream(firstStream) && isReadableStream(lastStream)) {
    resultStream = new DuplexWrapper({
      readStream: streams[streams.length - 1],
      writeStram: streams[0]
    });
  }
  else if (isWritableStream(firstStream)) {
    // Need to wrap the stream so we don't mix up the error signals
    resultStream = new WritableWrapper({
      writeStream: firstStream
    });
  }
  else if (isReadableStream(lastStream)) {
    // Need to wrap the stream so we don't mix up the error signals
    resultStream = new ReadableWrapper({
      readStream: lastStream
    });
  }
  else {
    assert(isReadableStream(firstStream));
    assert(isWritableStream(lastStream));

    // This is a full pipeline, from a readable into a writable, you can't
    // read or write at the endpoints. Return an event emitter with the
    // state updates.
    resultStream = new events.EventEmitter();

    // When all data has been passed to the end we will send a finish signal.
    // important not to pass on the 'end' signal from the reader because all
    // the data has not been handled then (if used with waitForStream).
    lastStream.on('finish', () => resultStream.emit('finish'));
  }

  return resultStream;
}

export default {};
