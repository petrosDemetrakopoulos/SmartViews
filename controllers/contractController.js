const stringify = require('fast-stringify');
let contract = null;
let mainTransactionObject = {};
const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const delay = require('delay');

function setContract (contractObject, account) {
    contract = contractObject;
    mainTransactionObject = {
        from: account,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
    cacheController.setContract(contractObject, account);
}

async function addFact (fact) {
    let addFactPromise = contract.methods.addFact(stringify(fact));
    return addFactPromise.send(mainTransactionObject, (err, txHash) => {
        helper.log('send:', err, txHash);
    }).on('error', (err) => {
        helper.log('error:', err);
        Promise.reject(err);
    }).on('transactionHash', (err) => {
        helper.log('transactionHash:', err);
    }).on('receipt', (receipt) => {
        helper.log('receipt:', receipt);
        Promise.resolve(receipt);
    })
}

function contractChecker (req, res, next) {
    if (contract) {
        next()
    } else {
        res.status(400);
        res.send({ status: 'ERROR', options: 'Contract not deployed' });
    }
}

async function getFactById (id) {
    return contract.methods.getFact(parseInt(id, 10)).call(function (err, result) {
        if (!err) {
            result = removeUnneededFieldsFromBCResponse(result);
            Promise.resolve(result);
        } else {
            helper.log(err);
            Promise.reject(err);
        }
    });
}

async function getGroupByWithId (id) {
    return contract.methods.getGroupBy(parseInt(id, 10)).call(function (err, result) {
        if (!err) {
            result = removeUnneededFieldsFromBCResponse(result);
            Promise.resolve(result);
        } else {
            helper.log(err);
            Promise.reject(err);
        }
    });
}

function getAllGroupbys (callback){
    let getGroupIdTimeStart = helper.time();
    contract.methods.groupId().call(function (err, result) {
        let getGroupIdTime = helper.time() - getGroupIdTimeStart;
        if(!err) {
            if (result > 0) {
                let getAllGBsFromBCTimeStart = helper.time();
                contract.methods.getAllGroupBys(result).call(function (err, resultGB) {
                    let getAllGBsTime = helper.time() - getAllGBsFromBCTimeStart;
                    let times = {getAllGBsTime: getAllGBsTime, getGroupIdTime: getGroupIdTime};
                    if (!err) {
                        resultGB = removeUnneededFieldsFromBCResponse(resultGB);
                        callback(null, resultGB, times);
                    } else {
                        helper.log(err);
                        callback(err);
                    }
                });
            } else {
                let times = {getGroupIdTime: getGroupIdTime};
                callback(null, [], times);
            }
        } else {
            helper.log(err);
            callback(err);
        }
    });
}

async function addManyFacts (facts, sliceSize, io) {
    helper.log('length = ' + facts.length);
    let allSlicesReady = [];
    if (sliceSize > 1) {
        let slices = [];
        let slicesNum = Math.ceil(facts.length / sliceSize);
        helper.log('*will add ' + slicesNum + ' slices*');
        for (let j = 0; j < slicesNum; j++) {
            if (j === 0) {
                slices[j] = facts.filter((fct, idx) => idx < sliceSize);
            } else {
                slices[j] = facts.filter((fct, idx) => idx > j * sliceSize && idx < (j + 1) * sliceSize);
            }
        }

        allSlicesReady = slices.map(slc => {
            return slc.map(fct => {
                return stringify(fct);
            });
        });
    } else {
        allSlicesReady = facts.map(fact => {
            return [stringify(fact)];
        });
    }

    let i = 1;
    for (const slc of allSlicesReady) {
        await contract.methods.addFacts(slc).send(mainTransactionObject, () => {
        }).on('error', (err) => {
            helper.log('error:', err);
        }).on('transactionHash', (hash) => {
            helper.log(i);
            io.emit('progress', i / allSlicesReady.length);
            i++;
        });
    }
    return Promise.resolve(true);
}

async function getAllFacts (factsLength) {
    let allFacts = [];
    for (let i = 0; i < factsLength; i++) {
        await contract.methods.facts(i).call(function (err, result2) {
            if (!err) {
                result2 = removeUnneededFieldsFromBCResponse(result2);
                if ('payload' in result2) {
                    let crnLn = JSON.parse(result2['payload']);
                    crnLn.timestamp = result2['timestamp'];
                    allFacts.push(crnLn);
                }
            } else {
                helper.log(err);
            }
        })
    }
    return allFacts;
}

async function getAllFactsHeavy (factsLength) {
    let allFacts = [];
    await contract.methods.getAllFacts(factsLength).call(function (err, result) {
        if (!err) {
            result = removeUnneededFieldsFromBCResponse(result);
            if ('payloads' in result) {
                for (let i = 0; i < result['payloads'].length; i++) {
                    let crnLn = JSON.parse(result['payloads'][i]);
                    crnLn.timestamp = result['timestamps'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            helper.log(err);
        }
    });
    return allFacts;
}

async function getFactsFromTo (from, to) {
    let allFacts = [];
    await contract.methods.getFactsFromTo(from, to).call(function (err, result) {
        if (!err) {
            result = removeUnneededFieldsFromBCResponse(result);
            if ('payloadsFromTo' in result) {
                for (let i = 0; i < result['payloadsFromTo'].length; i++) {
                    let crnLn = JSON.parse(result['payloadsFromTo'][i]);
                    crnLn.timestamp = result['timestampsFromTo'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            helper.log(err);
        }
    });
    return allFacts;
}

async function getFactsCount () {
    let id = -1;
    await contract.methods.dataId().call(function (err, result) {
        if (!err) {
            id = result;
        } else {
            helper.log(err);
        }
    });
    return id;
}

function getLatestId (callback) {
    contract.methods.dataId().call(function (err, latestId) {
        if(err){
            callback(err, null);
        } else {
            callback(null, latestId);
        }
    });
}

function deleteGBsById (gbIdsToDelete, callback) {

    let deleteGBsByIdPromise =  contract.methods.deleteGBsById(gbIdsToDelete);
    return deleteGBsByIdPromise.send(mainTransactionObject, (err, txHash) => {
        helper.log('send:', err, txHash);
    }).on('error', (err) => {
        helper.log('error:', err);
        Promise.reject(err);
    }).on('transactionHash', (err) => {
        helper.log('transactionHash:', err);
    }).on('receipt', (receipt) => {
        helper.log('receipt:', receipt);
        Promise.resolve(receipt);
    });
}

function removeUnneededFieldsFromBCResponse (bcResponse) {
    // for some unknown reason web3 responses contain all fields twice, therefore we have
    // to remove half of them to proceed, it is just a data preprocessing stage
    let len = Object.keys(bcResponse).length;
    for (let j = 0; j < len / 2; j++) {
        delete bcResponse[j];
    }
    return bcResponse;
}

function deleteCachedResults (sortedByEvictionCost, callback) {
    cacheController.deleteFromCache(sortedByEvictionCost, function (gbIdsToDelete) {
        deleteGBsById(gbIdsToDelete).then(receipt => {
            callback(null, receipt);
        }).catch(error => {
            helper.log(error);
            callback(error);
        });
    });
}

module.exports = {
    addManyFacts: addManyFacts,
    getAllFacts: getAllFacts,
    getAllFactsHeavy: getAllFactsHeavy,
    getFactsFromTo: getFactsFromTo,
    getFactsCount: getFactsCount,
    setContract: setContract,
    addFact: addFact,
    getFactById: getFactById,
    getGroupByWithId: getGroupByWithId,
    contractChecker: contractChecker,
    getLatestId: getLatestId,
    deleteGBsById: deleteGBsById,
    deleteCachedResults: deleteCachedResults,
    getAllGroupbys: getAllGroupbys
};
