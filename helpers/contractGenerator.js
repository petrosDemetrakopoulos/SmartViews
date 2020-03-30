const fs = require('fs');
const helper = require('../helpers/helper');
const handlebars = require('handlebars');

async function generateContract (templateFileName) {
    const factTbl = require('../templates/' + templateFileName);
    const createTable = factTbl.template.createTable;
    const tableName = factTbl.template.tableName;

    const templateFile = fs.readFileSync('contracts/contractTemplate.txt', 'utf8');
    const handlebarTemplate = handlebars.compile(templateFile);
    const contractResult = handlebarTemplate({ contract: { name: factTbl.name, structName: factTbl.structName } });
    return new Promise(function (resolve, reject) {
        fs.writeFile('contracts/' + factTbl.name + '.sol', contractResult, function (err) {
            if (err) {
                /* istanbul ignore next */
                console.log(err);
                /* istanbul ignore next */
                return reject(new Error('error'));
            }
            helper.log('******************');
            helper.log('Contract generated!');
            helper.log('******************');
            let templ = factTbl.template;
            return resolve({ msg: 'OK', filename: factTbl.name, template: templ, createTable: createTable, tableName: tableName });
        });
    });
}

module.exports = {
    generateContract: generateContract
};
