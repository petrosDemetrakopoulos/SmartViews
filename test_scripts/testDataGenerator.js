const fs = require('fs');
const helper = require('../helpers/helper');

let k = 0;
let all = [];
for (let i = 0; i < 10; i++) {
    for (let j = 0; j < 1000; j++) {
        let A = helper.getRandomInt(0, 400000);
        let B = helper.getRandomInt(0, 400000);
        let C = helper.getRandomInt(0, 400000);
        let D = helper.getRandomFloat(0, 400000);
        let newObj = { pk: k, A: A, B: B, C: C, D: D };
        all.push(newObj);
        console.log(k);
        k++;
    }
}
fs.writeFile('testData/10kfourcol.json', JSON.stringify(all), function (err) {
    if (err) {
        return console.log(err);
    }
    console.log('The file was saved!');
});
