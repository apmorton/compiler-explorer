// Copyright (c) 2019, Compiler Explorer Team
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
"use strict";

const
    bluebird = require('bluebird'),
    redis = require('redis'),
    uuidv1 = require('uuid/v1'),
    logger = require('../logger').logger,
    path = require('path');

bluebird.promisifyAll(redis);

class ExecutionStarter {
    constructor(redisUrl) {
        this.rcli = redis.createClient({
            url: redisUrl
        });
    }

    static getExecutionKey(hashedKey) {
        return hashedKey + '_' + uuidv1();
    }

    async execute(hashedKey, buildResult, execParams) {
        const startTime = process.hrtime();
        const executable = path.relative(buildResult.dirPath, buildResult.executableFilename);
        const execId = ExecutionStarter.getExecutionKey(hashedKey);
        const execResultId = execId + '_result';
        const message = JSON.stringify({
            hashedKey: hashedKey,
            executable: executable,
            execParams: execParams,
            resultKey: execResultId,
        });

        // duplicate redis client so we can use blocking operations
        const cli = await this.rcli.duplicateAsync();

        // put our execution request into redis
        await cli.batch()
            // key will expire after 60 seconds, preventing stale execution
            .set(execId, message, 'EX', 60)
            // push item to end of list to be atomically popped by execution clients
            .rpush('exec-queue', execId)
            .execAsync();

        // TODO: check for errors before blocking on response

        // block on execution response
        const response = await cli.blpopAsync(execResultId, 60);
        const data = response[1];
        const ret = JSON.parse(data);
        const elapsed = process.hrtime(startTime);
        logger.info('Execution time (hr): %ds %dms', elapsed[0], elapsed[1] / 1000000);
        return ret;
    }
}

module.exports = {
    ExecutionStarter
};
