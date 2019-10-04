const solc = require('solc');
const fs = require('fs');
let config = require('../config_private');
const configLab = require('../config_lab');
const Web3 = require('web3');
const web3 = new Web3(new Web3.providers.HttpProvider(config.blockchainIP));
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
            console.log('send:', err, txHash);
        })
        .on('error', (err) => {
            console.log('error:', err);
        })
        .on('transactionHash', (err) => {
            console.log('transactionHash:', err);
        })
        .on('receipt', (receipt) => {
            console.log('receipt:', receipt);
            contract.options.address = receipt.contractAddress;
            rec = receipt;
        });
    return { contractDeployed: { contractName: Object.keys(output.contracts)[0].slice(1), address: rec.contractAddress }, options: contractInstance.options, contractObject: contract };
}

module.exports = {
    deployContract: deploy
};
