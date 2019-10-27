const crypto = require('crypto');
let md5sum = crypto.createHash('md5');
const stringify = require('fast-stringify');
let config = require('../config_private');
const redis = require('redis');
const client = redis.createClient(config.redisPort, config.redisIP);
const Web3 = require('web3');
let contract = null;
let mainTransactionObject = {};
let redisConnected = false;
const helper = require('../helpers/helper');

function setContract (contractObject, account) {
    contract = contractObject;
    mainTransactionObject = {
        from: account,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
}

client.on('connect', function () {
    redisConnected = true;
    helper.log('Redis connected');
});
client.on('error', function (err) {
    redisConnected = false;
    helper.log('Something went wrong ' + err);
});

function saveOnCache (gbResult, operation, latestId) {
    md5sum = crypto.createHash('md5');
    md5sum.update(stringify(gbResult));
    let hash = md5sum.digest('hex');
    let gbResultSize = Object.keys(gbResult).length;
    let slicedGbResult = [];
    if (config.autoCacheSlice === 'manual') {
        if (gbResultSize > config.cacheSlice) {
            let crnSlice = [];
            let metaKeys = {
                operation: gbResult['operation'],
                groupByFields: gbResult['groupByFields'],
                field: gbResult['field'],
                viewName: gbResult['viewName']
            };
            for (const key of Object.keys(gbResult)) {
                if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'viewName') {
                    helper.log(key);
                    crnSlice.push({ [key]: gbResult[key] });
                    if (crnSlice.length >= config.cacheSlice) {
                        slicedGbResult.push(crnSlice);
                        crnSlice = [];
                    }
                }
            }
            if (crnSlice.length > 0) {
                slicedGbResult.push(crnSlice); // we have a modulo, slices are not all evenly dιstributed, the last one contains less than all the previous ones
            }
            slicedGbResult.push(metaKeys);
        }
    } else {
        // redis allows 512MB per stored string, so we divide the result of our gb with 512MB to find cache slice
        // maxGbSize is the max number of bytes in a row of the result
        let mb512InBytes = 512 * 1024 * 1024;
        let maxGbSize = config.maxGbSize;
        helper.log('Group-By result size in bytes = ' + gbResultSize * maxGbSize);
        helper.log('size a cache position can hold in bytes: ' + mb512InBytes);
        if ((gbResultSize * maxGbSize) > mb512InBytes) {
            let crnSlice = [];
            let metaKeys = {
                operation: gbResult['operation'],
                groupByFields: gbResult['groupByFields'],
                field: gbResult['field'],
                viewName: gbResult['viewName']
            };
            let rowsAddedInslice = 0;
            let crnSliceLengthInBytes = 0;
            for (const key of Object.keys(gbResult)) {
                if (key !== 'operation' && key !== 'groupByFields' && key !== 'field') {
                    helper.log(key);
                    crnSlice.push({ [key]: gbResult[key] });
                    rowsAddedInslice++;
                    crnSliceLengthInBytes = rowsAddedInslice * maxGbSize;
                    helper.log('Rows added in slice:');
                    helper.log(rowsAddedInslice);
                    if (crnSliceLengthInBytes === (mb512InBytes - 40)) { // for hidden character like backslashes etc
                        slicedGbResult.push(crnSlice);
                        crnSlice = [];
                    }
                }
            }
            if (crnSlice.length > 0) {
                slicedGbResult.push(crnSlice); // we have a modulo, slices are not all evenly dιstributed, the last one contains less than all the previous ones
            }
            slicedGbResult.push(metaKeys);
        } else {
            helper.log('NO SLICING NEEDED');
        }
    }
    let colSize = gbResult.groupByFields.length;
    let columns = stringify({ fields: gbResult.groupByFields });
    let num = 0;
    let crnHash = '';
    if (slicedGbResult.length > 0) {
        for (const slice in slicedGbResult) {
            crnHash = hash + '_' + num;
            helper.log(crnHash);
            client.set(crnHash, stringify(slicedGbResult[slice]), redis.print);
            num++;
        }
    } else {
        crnHash = hash + '_0';
        client.set(crnHash, stringify(gbResult), redis.print);
    }
    return contract.methods.addGroupBy(crnHash, Web3.utils.fromAscii(operation), latestId, colSize, gbResultSize, columns).send(mainTransactionObject);
}

function deleteFromCache (evicted, callback) {
    let keysToDelete = [];
    let gbIdsToDelete = [];
    if (config.cacheEvictionPolicy === 'FIFO') {
        for (let i = 0; i < config.maxCacheSize; i++) {
            keysToDelete.push(evicted[i].hash);
            let crnHash = evicted[i].hash;
            let cachedGBSplited = crnHash.split('_');
            let cachedGBLength = parseInt(cachedGBSplited[1]);
            if (cachedGBLength > 0) { // reconstructing all the hashes in cache if it is sliced
                for (let j = 0; j < cachedGBLength; j++) {
                    keysToDelete.push(cachedGBSplited[0] + '_' + j);
                }
            }
            gbIdsToDelete[i] = evicted[i].id;
        }
        helper.log('keys to remove from cache are:');
        helper.log(keysToDelete);
    }
    client.del(keysToDelete);
    callback(gbIdsToDelete);
}

function getManyCachedResults (allHashes, callback) {
    client.mget(allHashes, function (error, allCached) {
        if (error) {
            callback(error);
        } else {
            callback(null, allCached);
        }
    })
}

function getRedisStatus () {
    return redisConnected;
}

module.exports = {
    setContract: setContract,
    saveOnCache: saveOnCache,
    deleteFromCache: deleteFromCache,
    getManyCachedResults: getManyCachedResults,
    getRedisStatus: getRedisStatus
};