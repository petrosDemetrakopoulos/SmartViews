'use strict';
const solc = require('solc');
const fs = require('fs');
const path = require('path');
let config = require('../config_private');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.WebsocketProvider(config.blockchainIP));
const helper = require('../helpers/helper');

async function deploy (account, contractPath, contract) {
    const inputContract = fs.readFileSync(contractPath).toString();
    const fn = path.basename(contractPath);
    const contractName = fn.substr(0, fn.length - 4);
    let input = {
        language: 'Solidity',
        sources: { },
        settings: {
            outputSelection: {
                '*': {
                    '*': ['*']
                }
            }
        }
    };
    input.sources[fn] = { content: inputContract };
    const output = JSON.parse(solc.compile(JSON.stringify(input)));
    const bytecode = output.contracts[fn][contractName].evm.bytecode.object;
    const abi = output.contracts[fn][contractName].abi;
    let rec = {};
    contract = new web3.eth.Contract(abi);
    const contractInstance = await contract.deploy({ data: '0x' + bytecode })
        .send({
            from: account,
            gas: 150000000,
            gasPrice: '30000000000000'
        })
        .on('error', (err) => {
            /* istanbul ignore next */
            console.log('error:' + err);
        })
        .on('transactionHash', (txHash) => {
            helper.log('transactionHash:' + txHash);
        })
        .on('receipt', (receipt) => {
            helper.log('receipt:' + JSON.stringify(receipt));
            contract.options.address = receipt.contractAddress;
            rec = receipt;
        });
    return { contractDeployed: { contractName: contractName, address: rec.contractAddress },
        options: contractInstance.options,
        contractObject: contract
    };
}

module.exports = {
    deployContract: deploy
};
