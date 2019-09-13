const fs = require('fs');
const request = require('request');
let numberOfFacts = 100;
let fileToSaveTestData = 'testData_';
const helper = require('../helpers/helper');

let all = [];
let allSecondRun = [];
for (let j = 0; j < numberOfFacts; j++) {
    let A = helper.getRandomInt(0, 100);
    let Asec = helper.getRandomInt(0, 100);
    let B = helper.getRandomInt(0, 100);
    let Bsec = helper.getRandomInt(0, 100);
    let C = helper.getRandomInt(0, 100);
    let Csec = helper.getRandomInt(0, 100);
    let D = helper.getRandomFloat(0, 100);
    let Dsec = helper.getRandomInt(0, 100);
    let newObj = { pk: j, A: A, B: B, C: C, D: D };
    let newObjSec = { pk: j + numberOfFacts, A: Asec, B: Bsec, C: Csec, D: Dsec };
    all.push(newObj);
    allSecondRun.push(newObjSec);
}

fs.writeFile('testData/' + fileToSaveTestData + '1' + '.json', JSON.stringify(all), function (err) {
    if (err) {
        return console.log(err);
    }
    fs.writeFile('testData/' + fileToSaveTestData + '2' + '.json', JSON.stringify(allSecondRun), function (err) {
        if (err) {
            return console.log(err);
        }
        let url = 'http://localhost:3000/load_dataset/' + fileToSaveTestData + '1' + '.json';
        let urlSec = 'http://localhost:3000/load_dataset/' + fileToSaveTestData + '2' + '.json';
        let urlgbCountAB = 'http://localhost:3000/getViewByName/A|B|C|D(COUNT)';
        console.log('The file was saved!');
        // experiment scenario begins
        // 1) We add n records
        request({ url: url, method: 'GET' }, async function (error, httpResponse, body) {
            if (error) {
                return console.log(error);
            }
            console.log('added first records');
            // 2) We request group by A|B(COUNT)
            await request({ url: urlgbCountAB, method: 'GET' }, async function (error, httpResponse, body) {
                if (error) {
                    return console.log(error);
                }
                console.log('Run  1 completed');
                console.log('Number of facts = ' + numberOfFacts);
                console.log('Time results (s)');
                console.log('------------');
                let JSONresp = JSON.parse(body);
                if ('sqlTime' in JSONresp) {
                    console.log('SQL Time: ' + JSONresp.sqlTime.toFixed(5));
                }
                console.log('Blockchain Time: ' + JSONresp.bcTime.toFixed(5));
                if ('cacheSaveTime' in JSONresp) {
                    console.log('Cache save Time: ' + JSONresp.cacheSaveTime.toFixed(5));
                }
                if ('cacheRetrieveTime' in JSONresp) {
                    console.log('Cache retrieve Time: ' + JSONresp.cacheRetrieveTime.toFixed(5));
                }
                console.log('Total Time: ' + JSONresp.totalTime.toFixed(5));
                console.log('All total time: ' + JSONresp.allTotal.toFixed(5));
                console.log('**');
                // 3) We add n records again
                await request({ url: urlSec, method: 'GET' }, async function (error, httpResponse, body) {
                    console.log('%%%');
                    if (error) {
                        return console.log(error);
                    }
                    console.log('added second records');
                    // 4) We request group by A|B(COUNT) again
                    await request({ url: urlgbCountAB, method: 'GET' }, async function (error, httpResponse, body2) {
                        if (error) {
                            return console.log(error);
                        }
                        console.log('Run  2 completed');
                        console.log('Number of facts = ' + numberOfFacts);
                        console.log('Time results (s)');
                        console.log('------------');
                        let JSONresp = JSON.parse(body2);
                        if ('sqlTime' in JSONresp) {
                            console.log('SQL Time: ' + JSONresp.sqlTime.toFixed(5));
                        }
                        console.log('Blockchain Time: ' + JSONresp.bcTime.toFixed(5));
                        if ('cacheSaveTime' in JSONresp) {
                            console.log('Cache save Time: ' + JSONresp.cacheSaveTime.toFixed(5));
                        }
                        if ('cacheRetrieveTime' in JSONresp) {
                            console.log('Cache retrieve Time: ' + JSONresp.cacheRetrieveTime.toFixed(5));
                        }
                        console.log('Total Time: ' + JSONresp.totalTime.toFixed(5));
                        console.log('All total time: ' + JSONresp.allTotal.toFixed(5));
                    });
                });
            });
        });
    });
});
