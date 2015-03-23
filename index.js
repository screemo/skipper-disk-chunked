/**
 * Module dependencies
 */
var fsx = require('fs-extra')
    , path = require('path');



/**
 * skipper-disk
 *
 * @type {Function}
 * @param  {Object} options
 * @return {Object}
 */

module.exports = function ChunkedUpload(options) {

    //TODO: Parameters validation + logging

    var getFileTempDir = function(identifier) {
        return path.join(options.tmpDir, identifier)
    };

    var getChunkFileName = function(identifier, chunkNumber) {
        return path.join(getFileTempDir(identifier), identifier + '.' + chunkNumber);
    };

    var getChunkPath = function(identifier, chunkNumber) {
        return path.resolve(options.dirname, getChunkFileName(identifier, chunkNumber));
    };

    options = options || {};
    options.tmpDir = options.tmpDir || 'tmp';
    options.saveAs = getChunkFileName(options.fileIdentifier, options.chunkNumber);
    options.onFileComplete = options.onFileComplete || function(fd, callback) { return callback(null); };


    var diskAdapter = require('skipper-disk')(options);

    var log = options.log || function _noOpLog() {};

    var adapter = {};
    adapter.rm = diskAdapter.rm;
    adapter.ls = diskAdapter.ls;
    adapter.read = diskAdapter.read;



    /**
     * This function will combine all chunks into the out__ stream
     * @param out__
     * @param fileIdentifier
     * @param totalChunks
     * @param currentChunk
     * @param callback
     * @returns {*}
     */
    var combineChunks = function(out__, fileIdentifier, totalChunks, currentChunk, callback){

        if (currentChunk > totalChunks) {
            return callback(null);
        }

        var currentChunkPath = getChunkPath(fileIdentifier, currentChunk);


        fsx.exists(currentChunkPath, function(exists){

            if (!exists) {
                return callback('Could not find chunk ' + currentChunk + ' for file ' + fileIdentifier);
            }

            var __in = fsx.createReadStream(getChunkPath(fileIdentifier, currentChunk));

            __in.on('end', function() {
                currentChunk++;
                combineChunks(out__, fileIdentifier, totalChunks, currentChunk, callback);
            });

            __in.pipe(out__, { end : false});

        });

    };

    /**
     * Finish the upload, all chunks are here! Horray!
     * Combine them and cleanup
     *
     * @param finalFile
     * @param fileIdentifier
     * @param totalChunks
     * @param callback
     */
    var finalizeUpload = function(finalFile, fileIdentifier, totalChunks, callback) {
        var out__ = fsx.createWriteStream(finalFile);
        combineChunks(out__, fileIdentifier, totalChunks, 1, function(err){
            if (err) {
                return callback(err);
            }

            //Remove file temporary directory
            fsx.remove(path.resolve(options.dirname, getFileTempDir(fileIdentifier)), function(err) {
                if (err) {
                    return callback(err);
                }

                options.onFileComplete(finalFile, callback);
            });
        });
    };


    /**
     * This is the main override of skipper-disk. Here we intercept new files, change the file name (To the chunk file name)
     * And send to skipper-disk.
     *
     * @param options
     * @returns {*}
     */
    adapter.receive = function(options) {

        //Create a new skipper-disk outs__ stream
        var outs__ = diskAdapter.receive(options);

        //Save it's _write function as we are overriding it. (This will allow us to intercept the done() )
        var _outsWrite = outs__._write;

        //Here we will intercept the done() callback and perform further processing when the chunk upload is complete.
        //We will check if all chunks exist and if so combine them.
        outs__._write = function onFile(__newFile, encoding, done) {

            // List all FS chunks to see if upload is complete.
            var complete = function() {
                var currentChunk = 1;
                var testChunkExists = function() {
                    fsx.exists(path.resolve(options.dirname, getChunkFileName(options.fileIdentifier, currentChunk)), function(exists) {
                        if (exists) {
                            currentChunk++;
                            if (currentChunk > options.totalChunks) {   //We comepleted the upload! Combine files.
                                finalizeUpload(path.resolve(options.dirname, options.fileName), options.fileIdentifier, options.totalChunks, function(err) {
                                    if (err){
                                        return done(err);
                                    }

                                    done();
                                });
                            } else {
                                // Recursion
                                testChunkExists();
                            }
                        } else {
                            done();
                        }
                    });
                };
                testChunkExists();
            };

            //And now we pipe the stream to the original _write method only with our callback.
            _outsWrite(__newFile, encoding, complete);
        };

        //Return the outs__ stream
        return outs__;

    };

    //This will allow us to check the existance of a specific chunk to allow resumable uploads.
    adapter.chunkExists = function(done) {
        fsx.exists(path.resolve(options.dirname, getChunkFileName(options.fileIdentifier, options.chunkNumber)), function(exists) {
            return done(exists);
        });
    };


    return adapter;
};
