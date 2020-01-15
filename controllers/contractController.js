const stringify = require('fast-stringify');
let contract = null;
let mainTransactionObject = {};
const helper = require('../helpers/helper');
const cacheController = require('./cacheController');

function setContract (contractObject, account) {
    contract = contractObject;
    mainTransactionObject = helper.getMainTransactionObject(account);
    cacheController.setContract(contractObject, account);
}

async function addFact (fact) {
    const addFactPromise = contract.methods.addFact(stringify(fact));
    return sendTransactionWithContractMethod(addFactPromise);
}

function contractChecker (req, res, next) {
    if (contract) {
        next()
    } else {
        res.status(400);
        res.send({ status: 'ERROR', message: 'Contract not deployed' });
    }
}

const bcResponseHandler = function (err, result) {
    if (!err) {
        result = removeUnneededFieldsFromBCResponse(result);
        Promise.resolve(result);
    } else {
        /* istanbul ignore next */
        helper.log(err);
        /* istanbul ignore next */
        Promise.reject(err);
    }
};

async function getFactById (id) {
    return contract.methods.getFact(parseInt(id, 10)).call(function (err, result) {
        bcResponseHandler(err, result);
    });
}

async function getGroupByWithId (id) {
    return contract.methods.getGroupBy(parseInt(id, 10)).call(function (err, result) {
        bcResponseHandler(err, result);
    });
}

async function getAllGroupbys () { // promisify it to await where we call it
    const getGroupIdTimeStart = helper.time();
    return new Promise((resolve, reject) => {
        contract.methods.groupId().call(function (err, result) {
            const getGroupIdTime = helper.time() - getGroupIdTimeStart;
            if (!err) {
                if (result > 0) {
                    const getAllGBsFromBCTimeStart = helper.time();
                    return contract.methods.getAllGroupBys(result).call(function (err, resultGB) {
                        const getAllGBsTime = helper.time() - getAllGBsFromBCTimeStart;
                        const times = { getAllGBsTime: getAllGBsTime, getGroupIdTime: getGroupIdTime };
                        if (!err) {
                            resultGB = removeUnneededFieldsFromBCResponse(resultGB);
                            resultGB.times = times;
                            resolve(resultGB);
                        } else {
                            /* istanbul ignore next */
                            helper.log(err);
                            /* istanbul ignore next */
                            reject(err);
                        }
                    });
                } else {
                    const times = { times: { getGroupIdTime: getGroupIdTime, getAllGBsTime: 0 } };
                    resolve(times);
                }
            } else {
                /* istanbul ignore next */
                helper.log(err);
                /* istanbul ignore next */
                reject(err);
            }
        });
    });
}

async function addManyFacts (facts, sliceSize, io) {
    helper.log('length = ' + facts.length);
    let allSlicesReady = [];
    if (sliceSize > 1) {
        let slices = [];
        const slicesNum = Math.ceil(facts.length / sliceSize);
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
            /* istanbul ignore next */
            helper.log('error:', err);
        }).on('transactionHash', (hash) => {
            helper.log(i);
            io.emit('progress', i / allSlicesReady.length);
            i++;
        });
    }
    return Promise.resolve(true);
}


async function getAllFactsHeavy (factsLength) {
    let allFacts = [];
    await contract.methods.getAllFacts(factsLength).call(function (err, result) {
        if (!err) {
            result = removeUnneededFieldsFromBCResponse(result);
            if ('payloads' in result) {
                for (let i = 0; i < result['payloads'].length; i++) {
                    const crnLn = JSON.parse(result['payloads'][i]);
                    crnLn.timestamp = result['timestamps'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            /* istanbul ignore next */
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
                    const crnLn = JSON.parse(result['payloadsFromTo'][i]);
                    crnLn.timestamp = result['timestampsFromTo'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            /* istanbul ignore next */
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
            /* istanbul ignore next */
            helper.log(err);
        }
    });
    return id;
}

async function getLatestId () {
    return new Promise((resolve, reject) => {
        contract.methods.dataId().call(function (err, latestId) {
            if (err) {
                /* istanbul ignore next */
                console.log(err);
                /* istanbul ignore next */
                reject(err);
            } else {
                resolve(latestId);
            }
        });
    });
}

function deleteGBsById (gbIdsToDelete) {
    const deleteGBsByIdPromise = contract.methods.deleteGBsById(gbIdsToDelete);
    return sendTransactionWithContractMethod(deleteGBsByIdPromise);
}

function removeUnneededFieldsFromBCResponse (bcResponse) {
    // for some unknown reason web3 responses contain all fields twice, therefore we have
    // to remove half of them to proceed, it is just a data preprocessing stage
    const len = Object.keys(bcResponse).length;
    for (let j = 0; j < len / 2; j++) {
        delete bcResponse[j];
    }
    return bcResponse;
}

function deleteCachedResults (sortedByEvictionCost) {
    return new Promise((resolve, reject) => {
        cacheController.deleteFromCache(sortedByEvictionCost).then(gbIdsToDelete => {
            helper.log('IDS DELETED FROM CACHE:');
            helper.log(gbIdsToDelete);
            helper.log('*********');
            deleteGBsById(gbIdsToDelete).then(receipt => {
                resolve(receipt);
            }).catch(error => {
                /* istanbul ignore next */
                helper.log(error);
                /* istanbul ignore next */
                reject(error);
            });
        });
    });
}

function sendTransactionWithContractMethod (contractMethod) {
    return contractMethod.send(mainTransactionObject, (err, txHash) => {
        helper.log('send:', err, txHash);
    }).on('error', (err) => {
        /* istanbul ignore next */
        helper.log('error:', err);
        /* istanbul ignore next */
        Promise.reject(err);
    }).on('transactionHash', (txHash) => {
        helper.log('transactionHash:', txHash);
    }).on('receipt', (receipt) => {
        helper.log('receipt:', receipt);
        Promise.resolve(receipt);
    });
}

module.exports = {
    addManyFacts: addManyFacts,
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
