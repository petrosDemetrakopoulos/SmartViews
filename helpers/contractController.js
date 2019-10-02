const express = require('express');
const stringify = require('fast-stringify');
const app = express();
let http = require('http').Server(app);
let io = require('socket.io')(http);
let contract = null;
let mainTransactionObject = {};
function setContract(contractObject, account){
    contract = contractObject;
    mainTransactionObject = {
        from: account,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
}

async function addManyFacts (facts, sliceSize) {
    console.log('length = ' + facts.length);
    let allSlicesReady = [];
    if (sliceSize > 1) {
        let slices = [];
        let slicesNum = Math.ceil(facts.length / sliceSize);
        console.log('*will add ' + slicesNum + ' slices*');
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
        await contract.methods.addFacts(slc).send(mainTransactionObject, (err, txHash) => {
        }).on('error', (err) => {
            console.log('error:', err);
        }).on('transactionHash', (hash) => {
            console.log(i);
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
                let len = Object.keys(result2).length;
                for (let j = 0; j < len / 2; j++) {
                    delete result2[j];
                }
                // console.log('got fact ' + i);
                if ('payload' in result2) {
                    let crnLn = JSON.parse(result2['payload']);
                    crnLn.timestamp = result2['timestamp'];
                    allFacts.push(crnLn);
                }
            } else {
                console.log(err);
            }
        })
    }
    return allFacts;
}

async function getAllFactsHeavy (factsLength) {
    let allFacts = [];
    await contract.methods.getAllFacts(factsLength).call(function (err, result) {
        if (!err) {
            let len = Object.keys(result).length;
            for (let j = 0; j < len / 2; j++) {
                delete result[j];
            }
            if ('payloads' in result) {
                for (let i = 0; i < result['payloads'].length; i++) {
                    let crnLn = JSON.parse(result['payloads'][i]);
                    crnLn.timestamp = result['timestamps'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            console.log(err);
        }
    });
    return allFacts;
}

async function getFactsFromTo (from, to) {
    let allFacts = [];
    await contract.methods.getFactsFromTo(from, to).call(function (err, result) {
        if (!err) {
            let len = Object.keys(result).length;
            for (let j = 0; j < len / 2; j++) {
                delete result[j];
            }
            if ('payloadsFromTo' in result) {
                for (let i = 0; i < result['payloadsFromTo'].length; i++) {
                    let crnLn = JSON.parse(result['payloadsFromTo'][i]);
                    crnLn.timestamp = result['timestampsFromTo'][i];
                    allFacts.push(crnLn);
                }
            }
        } else {
            console.log(err);
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
            console.log(err);
        }
    });
    return id;
}

module.exports = {
    addManyFacts: addManyFacts,
    getAllFacts:getAllFacts,
    getAllFactsHeavy: getAllFactsHeavy,
    getFactsFromTo: getFactsFromTo,
    getFactsCount: getFactsCount,
    setContract: setContract
};