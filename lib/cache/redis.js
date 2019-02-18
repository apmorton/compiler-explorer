// Copyright (c) 2018, Compiler Explorer Authors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

const BaseCache = require('./base.js');
const redis = require('redis');
const Sentry = require('@sentry/node');
const logger = require('../logger').logger;

require('bluebird').promisifyAll(redis);

function messageFor(e) {
    return e.message || e.toString();
}

class RedisCache extends BaseCache {
    constructor(url) {
        super(`RedisCache(${url})`);
        this.cli = redis.createClient({
            url: url,
            returnBuffers: true,
        });
    }

    getInternal(key) {
        return this.cli.getAsync(key)
            .then(result => {
                return {hit: true, data: result};
            })
            .catch(e => {
                Sentry.captureException(e);
                logger.error("Error while trying to read Redis cache:", messageFor(e));
                return {hit: false};
            });
    }

    putInternal(key, value) {
        return this.cli.setAsync(key, value)
            .catch(e => {
                Sentry.captureException(e);
                logger.error("Error while trying to write to Redis cache:", messageFor(e));
            });
    }
}

module.exports = RedisCache;
