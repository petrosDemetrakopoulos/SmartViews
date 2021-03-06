'use strict';
const app = require('../index');
const expect = require('chai').expect;
const describe = require('mocha').describe;
const after = require('mocha').after;
const before = require('mocha').before;
const it = require('mocha').it;
const request = require('supertest');
const chai = require('chai');
const chaiHttp = require('chai-http');
const microtime = require('microtime');
const fs = require('fs');
let start = 0;
let stop = 0;
let initialConfig = {
    'recordsSlice': 1000,
    'cacheEvictionPolicy': 'FIFO',
    'calculationCostFunction': 'costFunction',
    'maxCacheSize': 1000000000000,
    'maxCacheSizeInKB': 1000000000,
    'cacheSlice': 2,
    'autoCacheSlice': 'auto',
    'maxGbSize': 100000,
    'redisPort': 6379,
    'redisIP': '127.0.0.1',
    'blockchainIP': 'http://localhost:8545',
    'sql': {
        'host': 'localhost',
        'user': 'root',
        'password': 'Xonelgataandrou1!',
        'database': 'Ptychiaki'
    },
    'cacheEnabled': true,
    'logging': false
};
chai.use(chaiHttp);
let responseBodyContractgeneration = {};
let responseBodyContractDeployment = {};

function freeze (time) {
    const stop = new Date().getTime() + time;
    while (new Date().getTime() < stop);
}
before(function (done) {
    console.log('Waiting for services to start...');
    setTimeout(done, 4000);
});
describe('testing default route', function () {
    before(function () {
        fs.writeFile('./config_private.json', JSON.stringify(initialConfig, null, 4), function (err) {
            if (err) throw err;
        });
    });
    start = microtime.nowDouble();
    it('should return OK status', async () => {
        return request(app)
            .get('/')
            .then(function (response) {
                expect(response.status).to.equal(200);
            });
    });
    it('should be html', async () => {
        return request(app)
            .get('/')
            .then(function (response) {
                expect(response).to.be.html;
            });
    });
});

describe('testing /dashboard route', function () {
    it('should return OK status', async () => {
        return request(app)
            .get('/dashboard')
            .then(function (response) {
                expect(response.status).to.equal(200);
            });
    });
    it('should be html', async () => {
        return request(app)
            .get('/dashboard')
            .then(function (response) {
                expect(response).to.be.html;
            });
    });
});

describe('testing /addFact route with NOT DEPLOYED contract', function () {
    let payload = { pk: 250, A: 1, B: 2, C: 3, D: 12.3 };
    let resp = {};
    it('should return error code 400', async () => {
        return request(app)
            .post('/addFact')
            .send(payload)
            .set('Accept', 'application/json')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(400);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });

    it('should have property "status" ', function () {
        expect(resp).to.have.property('status');
    });

    it('"status" should equal "ERROR" ', function () {
        expect(resp.status).to.equal('ERROR');
    });

    it('should have property "message" ', function () {
        expect(resp).to.have.property('message');
    });

    it('"message" should equal "Contract not deployed" ', function () {
        expect(resp.message).to.equal('Contract not deployed');
    });
});

describe('testing /new_contract/:fn route', function () {
    it('should return OK status', async () => {
        return request(app)
            .get('/new_contract/ABCDE.json')
            .then(function (response) {
                responseBodyContractgeneration = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(responseBodyContractgeneration).to.be.a('object');
    });

    it('should have property "message"', function () {
        expect(responseBodyContractgeneration).to.have.property('message');
    });

    it('"message" should equal "OK"', function () {
        expect(responseBodyContractgeneration.message).to.equal('OK');
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
    it('should return OK status', async () => {
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
    it('should return OK status', async () => {
        return request(app)
            .get('/form/ABCDE')
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
    let payload = { pk: 250, A: 1, B: 2, C: 3, D: 12.3 };
    let resp = {};
    it('should return OK status', async () => {
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
    it('should return OK status', async () => {
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
    it('should return OK status', async () => {
        return request(app)
            .get('/load_dataset/10fourcol.json')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });

    it('should have property "message"', function () {
        expect(resp).to.have.property('message');
    });

    it('"message" should equal "OK"', function () {
        expect(resp.message).to.equal('OK');
    });
});

describe('testing /allfacts', function () {
    setTimeout(function () { console.log('waiting...'); }, 1000);
    // wait so that latest fact should not have the same timestamp with the group by that will be cached
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/allfacts')
            .then(function (response) {
                resp = JSON.parse(response.text);
                expect(response.status).to.equal(200);
            });
    });

    it('should be an array', function () {
        expect(resp).to.be.an('array');
    });

    it('should have length of 11', function () {
        expect(resp).to.have.lengthOf(13);
    });
});

describe('testing /getFactsFromTo/:from/:to', function () {
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getFactsFromTo/2/5')
            .then(function (response) {
                resp = JSON.parse(response.text);
                expect(response.status).to.equal(200);
            });
    });

    it('should be an array', function () {
        expect(resp).to.be.an('array');
    });

    it('should have length of 4', function () {
        expect(resp).to.have.lengthOf(4);
    });
});

describe('testing /getViewByName/:viewName/:contract -- Initial query', function () {
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/AB(COUNT)/ABCDE')
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
    it('should return OK status', async () => {
        freeze(1000);
        return request(app)
            .get('/getViewByName/A(COUNT)/ABCDE')
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
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/AB(SUM-D)/ABCDE')
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
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/AB(MAX-D)/ABCDE')
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
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/AB(MIN-D)/ABCDE')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- AVERAGE', function () {
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/AB(AVERAGE-D)/ABCDE')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- AVERAGE (Reduction from cache)', function () {
    freeze(1000);
    let resp = {};
    it('should return OK status', async () => {
        return request(app)
            .get('/getViewByName/A(AVERAGE-D)/ABCDE')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached + Deltas -- SUM', async function () {
    let resp = {};
    before(async function () {
        await request(app).get('/load_dataset/10fourcol_b.json');
    });

    it('should return OK status', async function () {
        const resQuery = await request(app).get('/getViewByName/AB(SUM-D)/ABCDE');
        expect(resQuery.status).to.equal(200);
        resp = resQuery.text;
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached + Deltas -- AVERAGE', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(1000);
        return request(app)
            .get('/load_dataset/10fourcol_c') // adding deltas
            .then(function (response) {
                return request(app)
                    .get('/getViewByName/AB(AVERAGE-D))/ABCDE')
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

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached + Deltas -- MIN', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(1000);
        return request(app)
            .get('/load_dataset/10fourcol_d') // adding deltas
            .then(function (response) {
                return request(app)
                    .get('/getViewByName/AB(MIN-D))/ABCDE')
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

describe('testing /getViewByName/:viewName/:contract -- Same with previous cached', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(1000);
        return request(app)
            .get('/getViewByName/AB(COUNT)/ABCDE')
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
        freeze(1000);
        return request(app)
            .get('/load_dataset/10fourcol_e') // adding deltas
            .then(function (response) {
                return request(app)
                    .get('/getViewByName/AB(COUNT)/ABCDE')
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
        freeze(1000);
        return request(app)
            .get('/load_dataset/10fourcol_f') // adding deltas
            .then(function (response) {
                freeze(1000);
                return request(app)
                    .get('/getViewByName/A(COUNT)/ABCDE')
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
        freeze(1000);
        return request(app)
            .get('/getViewByName/C|D(COUNT)/ABCDE')
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
        freeze(1000);
        return request(app)
            .get('/getViewByName/notValidViewName/ABCDE')
            .then(function (response) {
                resp = response.body;
                expect(response.status).to.equal(200);
            });
    });

    it('should be an object', function () {
        expect(resp).to.be.a('object');
    });

    it('should have property "message" ', function () {
        expect(resp).to.have.property('message');
    });

    it('"message" should equal "view not found"', function () {
        expect(resp.message).to.equal('view not found');
    });

    it('should have property "status" ', function () {
        expect(resp).to.have.property('status');
    });

    it('"status" should equal "ERROR"', function () {
        expect(resp.status).to.equal('ERROR');
    });
});

describe('testing /getViewByName/:viewName/:contract -- manual slicing', function () {
    let resp = {};
    let config = require('../config_private');
    before(function () {
        config.autoCacheSlice = 'manual';
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
        });
    });

    it('should return OK status', function () {
        return request(app)
            .get('/load_dataset/10fourcol_g.json')
            .then(function (response) {
                freeze(1000);
                return request(app)
                    .get('/getViewByName/AB(COUNT)/ABCDE')
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

describe('testing /groupbyId/:id route', function () {
    let resp = {};
    it('should return OK status', function () {
        freeze(1000);
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

describe('testing /getViewByName/:viewName/:contract -- Deltas have no unique primary key', async function () {
    let resp = {};
    it('should return OK status', async function () {
        freeze(1000);
        return request(app)
            .get('/load_dataset/10fourcol_h.json') // adding deltas
            .then(function (response) {
                freeze(1000);
                return request(app)
                    .get('/getViewByName/A(COUNT)/ABCDE')
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

describe('testing /getViewByName/:viewName/:contract -- cache disabled', function () {
    let resp = {};
    let config = require('../config_private');
    before(function () {
        config.cacheEnabled = false;
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
        });
    });

    it('should return OK status', async function () {
        freeze(1000);
        return request(app)
            .get('/getViewByName/A(COUNT)/ABCDE')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });
});

describe('testing /getViewByName/:viewName/:contract -- calculationCostFunction = dataCubeDistanceFunction', function () {
    let resp = {};
    let config = require('../config_private');
    before(function () {
        config.cacheEnabled = true;
        config.calculationCostFunction = 'dataCubeDistance';
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
        });
    });

    it('should return OK status', async function () {
        return request(app)
            .get('/load_dataset/10fourcol_i.json') // adding deltas
            .then(function (response) {
                freeze(1000);
                return request(app)
                    .get('/getViewByName/A(COUNT)/ABCDE')
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

describe('testing /getViewByName/:viewName/:contract -- cacheEvictionPolicy = FIFO', function () {
    let resp = {};
    let config = require('../config_private');
    before(function () {
        config.cacheEnabled = true;
        config.calculationCostFunction = 'calculationCost';
        config.cacheEvictionPolicy = 'FIFO';
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
        });
    });

    it('should return OK status', async function () {
        return request(app)
            .get('/load_dataset/10fourcol_j.json') // adding deltas
            .then(function (response) {
                freeze(1000);
                return request(app)
                    .get('/getViewByName/A(COUNT)/ABCDE')
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

describe('testing /getcount route', function () {
    let resp = {};
    let config = require('../config_private');
    it('should return OK status', function () {
        freeze(1000);
        return request(app)
            .get('/getcount')
            .then(function (response) {
                resp = response.text;
                expect(response.status).to.equal(200);
                stop = microtime.nowDouble();
            });
    });

    it('should be a string', function () {
        expect(resp).to.be.a('string');
    });

    it('should equal "111" ', function () {
        expect(resp).to.equal('111');
    });

    after(function (done) {
        config.cacheEnabled = true;
        config.autoCacheSlice = 'auto';
        config.calculationCostFunction = 'costFunction';
        config.cacheEvictionPolicy = 'costFunction';
        fs.writeFile('./config_private.json', JSON.stringify(config, null, 4), function (err) {
            if (err) throw err;
            let time = stop - start;
            console.log('TEST RAN IN: ' + time.toFixed(2) + ' s');
            done();
        });
    });
});
