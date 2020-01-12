const solc = require('solc');
const fs = require('fs');
let config = require('../config_private');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.WebsocketProvider(config.blockchainIP));
const helper = require('../helpers/helper');


async function deploy (account, contractPath, contract) {
    const input = fs.readFileSync(contractPath);
    const output = solc.compile(input.toString(), 1);
    const bytecode = output.contracts[Object.keys(output.contracts)[0]].bytecode;
    const abi = JSON.parse(output.contracts[Object.keys(output.contracts)[0]].interface);
    let rec = {};
    contract = new web3.eth.Contract(abi);
    let contractInstance = await contract.deploy({ data: '0x' + bytecode })
        .send({
            from: account,
            gas: 150000000,
            gasPrice: '30000000000000'
        }, (err, txHash) => {
            if (err) {
                helper.log('send:' + err);
            } else {
                helper.log('send:' + txHash);
            }
        })
        .on('error', (err) => {
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
    return { contractDeployed: { contractName: Object.keys(output.contracts)[0].slice(1), address: rec.contractAddress },
        options: contractInstance.options,
        contractObject: contract
    };
}

module.exports = {
    deployContract: deploy
};
