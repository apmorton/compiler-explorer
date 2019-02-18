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
    logger = require('../logger').logger;

bluebird.promisifyAll(redis);

class ExecutionProcessor {
    constructor(redisUrl) {
        this.rcli = redis.createClient({
            url: redisUrl
        });
    }

    async execute(workCallback) {
        const queueRes = await this.rcli.blpopAsync('exec-queue', 0);

        const startTime = process.hrtime();
        const executionId = queueRes[1];
        logger.debug("Execution ID: ", executionId);

        const batchRes = await this.rcli.batch()
            .get(executionId)
            .del(executionId)
            .execAsync();

        const getRes = batchRes[0];
        if (getRes === null) {
            logger.debug("Stale Execution ID: ", executionId);
            return;
        }

        const message = JSON.parse(getRes);
        const result = await workCallback(message.hashedKey, message.executable, message.execParams);
        const resultMessage = JSON.stringify(result);

        await this.rcli.multi()
            .rpush(message.resultKey, resultMessage)
            .expire(message.resultKey, 30)
            .execAsync();

        const elapsed = process.hrtime(startTime);
        logger.info('Execution time (hr): %ds %dms', elapsed[0], elapsed[1] / 1000000);
    }
}

module.exports = {
    ExecutionProcessor
};
