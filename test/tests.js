const app = require('../index');
const expect = require('chai').expect;
const request = require('supertest');
const chai = require('chai');
const chaiHttp = require('chai-http');
const fs = require('fs');
chai.use(chaiHttp);
let responseBodyContractgeneration = {};
let responseBodyContractDeployment = {};
function freeze (time) {
    const stop = new Date().getTime() + time;
    while(new Date().getTime() < stop);
}
before(function(done) {
    console.log("Waiting for services to start...");
    setTimeout(done, 3000);
});
describe('testing default route', function () {
    it('should return OK status', function () {
        return request(app)
            .get('/')
            .then(function (response) {
                expect(response.status).to.equal(200);
            });
    });
    it('should be html', function () {
        return request(app)
            .get('/')
            .then(function (response) {
                expect(response).to.be.html;
            });
    });
});

describe('testing /dashboard route', function () {
    it('should return OK status', function () {
        return request(app)
            .get('/dashboard')
            .then(function(response) {
                expect(response.status).to.equal(200);
            });
    });
    it('should be html', function () {
        return request(app)
            .get('/dashboard')
            .then(function (response) {
                expect(response).to.be.html;
            });
    });
});

describe('testing /new_contract/:fn route', function () {
    it('should return OK status', function () {
        return request(app)
            .get('/new_contract/ABCD.json')
            .then(function(response) {
                responseBodyContractgeneration = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should have property "filename" ', function () {
        expect(responseBodyContractgeneration).to.have.property('filename');
    });

    it('should have property "template" ', function () {
        expect(responseBodyContractgeneration).to.have.property('template');
    });

    it('"template" should have object value', function () {
        expect(responseBodyContractgeneration.template).to.be.a('object');
    });
});

describe('testing /deployContract/:fn route', function () {
    it('should return OK status', function () {
        return request(app)
            .get('/deployContract/' + responseBodyContractgeneration.filename)
            .then(function (response) {
                responseBodyContractDeployment = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should have property "options" ', function () {
        expect(responseBodyContractDeployment).to.have.property('options');
    });

    it('"options" should have object value', function () {
        expect(responseBodyContractDeployment.options).to.be.a('object');
    });
});

describe('testing /form/:contract route', function () {
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/form/ABCD')
            .then(function (response) {
                resp = response;
                expect(response.status).to.equal(200);
            });
    });

    it('should be html', function () {
        expect(resp).to.be.html;
    });
});

describe('testing /addFact route', function () {
    let payload = {pk: 250, A: 1, B: 2, C: 3, D:12.3};
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .post('/addFact')
            .send(payload)
            .set('Accept', 'application/json')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should have property "transactionHash"', function () {
        expect(resp).to.have.property('transactionHash');
    });
});

describe('testing /getFactById/:id route', function () {
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getFactById/0')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });
});

describe('testing /load_dataset/:dt route', function () {
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/load_dataset/10fourcol')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });
});

describe('testing /allfacts', function () {
    setTimeout(function () { console.log('waiting...'); }, 4000);
    // wait so that latest fact should not have the same timetamp with the group by that will be cached
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/allfacts')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an array', function () {
        expect(resp).to.be.a('array');
    });

    it('should have length of 12', function () {
        expect(resp).to.have.lengthOf(12);
    });
});

describe('testing /getFactsFromTo/:from/:to', function () {
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getFactsFromTo/2/5')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an array', function () {
        expect(resp).to.be.a('array');
    });

    it('should have length of 4', function () {
        expect(resp).to.have.lengthOf(4);
    });
});

describe('testing /getViewByName/:viewName/:contract -- Initial query', function () {
    freeze(4000);
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getViewByName/A|B(COUNT)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- SUM', function () {
    freeze(4000);
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getViewByName/A|B(SUM-D)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- MAX', function () {
    freeze(4000);
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getViewByName/A|B(MAX-D)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- MIN', function () {
    freeze(4000);
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getViewByName/A|B(MIN-D)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(4000);
        return request(app)
            .get('/getViewByName/A|B(COUNT)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Reduction from cache without deltas', async function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(4000);
        return request(app)
            .get('/getViewByName/A(COUNT)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached + Deltas', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(4000);
        return request(app)
            .get('/load_dataset/10fourcol_b') // adding deltas
            .then(function (response) {
                return request(app)
                    .get('/getViewByName/A|B(COUNT)/ABCD')
                    .then(function (response) {
                        resp = response.text;
                        expect(response.status).to.equal(200);
                    });
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Reduction from cache + Deltas', async function () {
    let resp = {};
    it('should return OK status', async function () {
        freeze(4000);
        return request(app)
            .get('/load_dataset/10fourcol_c') // adding deltas
            .then(function (response) {
                freeze(4000);
                return request(app)
                    .get('/getViewByName/A(COUNT)/ABCD')
                    .then(function (response) {
                        resp = response.text;
                        expect(response.status).to.equal(200);
                    });
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- No requested fields belong to some cached', function () {
    let resp = {};
    it('should return OK status', function () {
        return request(app)
            .get('/getViewByName/C|D(COUNT)/ABCD')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Invalid view name', function () {
    let resp = {};
    it('should return OK status', async function () {
        freeze(4000);
        return request(app)
            .get('/getViewByName/notValidViewName/ABCD')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });

    it('should have property "error" ', function () {
        expect(resp).to.have.property('error');
    });
});

describe('testing /getViewByName/:viewName/:contract -- cache disabled', function () {
    let resp = {};
    let config = require('../config_private');
    before(function(done) {
        config.cacheEnabled = false;
        fs.writeFile('./config_private.json', JSON.stringify(config, null,4), function (err) {
            if (err) throw err;
            done();
        });
    });

    it('should return OK status', async function () {
        freeze(4000);
        return request(app)
            .get('/getViewByName/A(COUNT)/ABCD')
            .then(function (response) {
                config.cacheEnabled = true;
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /groupbyId/:id route', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(4000);
        return request(app)
            .get('/groupbyId/0')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

// describe('testing /getViewByName/:viewName/:contract -- Deltas have no unique primary key',async function() {
//     let resp = {};
//     it('should return OK status',async function() {
//         freeze(4000);
//         return request(app)
//             .get('/load_dataset/10fourcol_c') // adding deltas
//             .then(function(response) {
//                 freeze(4000);
//                 return request(app)
//                     .get('/getViewByName/A(COUNT)/ABCD')
//                     .then(function(response) {
//                         resp = response.text;
//                         console.log(resp);
//                         expect(response.status).to.equal(200);
//                     });
//             });
//     });
//
//     it('should be a string', function() {
//         expect(resp).to.be.a('string');
//     });
// });

describe('testing /getcount route', function () {
    let resp = {};
    let config = require('../config_private');
    it('should return OK status', function () {
        freeze(4000);
        return request(app)
            .get('/getcount')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
    after(function (done) {
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
            done();
        });
    });
});
