const fs = require('fs');
const helper = require('../helpers/helper');

let k = 101;
let all = [];
for (let i = 0; i < 10; i++) {
    for (let j = 0; j < 20; j++) {
        let A = helper.getRandomInt(0, 100);
        let B = helper.getRandomInt(0, 100);
        let C = helper.getRandomInt(0, 100);
        let D = helper.getRandomFloat(0, 100);
        let newObj = { pk: k, A: A, B: B, C: C, D: D };
        all.push(newObj);
        console.log(k);
        k++;
    }
}
fs.writeFile('test_data/200fourcol.json', JSON.stringify(all), function (err) {
    if (err) {
        return console.log(err);
    }
    console.log('The file was saved!');
});
