const fs = require('fs');

async function generateContract (templateFileName) {
    let fact_tbl = require('../templates/' + templateFileName);
    console.log(fact_tbl);
    let createTable = fact_tbl.template.create_table;
    let tableName = fact_tbl.template.table_name;
    let contrPayload = '';
    let firstLine = 'pragma solidity ^0.4.24;\npragma experimental ABIEncoderV2;\n';
    let secondLine = 'contract ' + fact_tbl.name + ' { \n';
    let thirdLine = '\tuint public dataId;\n';
    let fourthLine = '\tuint public groupId;\n\n';
    let sixthLine = '\tuint public viewId;\n\n';
    let fifthLine = '\tuint public lastCount;\n' +
        '\tuint public lastSUM;\n' +
        '\tuint public lastMin;\n' +
        '\tuint public lastMax;\n' +
        '\tuint public lastAverage;\n' +
        '\tbytes32 MIN_LITERAL = "MIN";\n' +
        '\tbytes32 MAX_LITERAL = "MAX";\n' +
        '\tbytes32 AVERAGE_LITERAL = "AVERAGE";\n' +
        '\tbytes32 COUNT_LITERAL = "COUNT";\n' +
        '\tbytes32 SUM_LITERAL = "SUM";\n';
    let constr = '\tconstructor() {\n' +
        '\t\tdataId = 0;\n' +
        '\t\tgroupId = 0;\n' +
        '\t\tlastCount = 0;\n' +
        '\t\tlastSUM = 0;\n' +
        '\t\tlastMin = 0;\n' +
        '\t\tlastMax = 0;\n' +
        '\t\tlastAverage = 0;\n' +
        '\t}\n';
    let properties = '';
    let struct = '\tstruct ' + fact_tbl.struct_Name + '{ \n';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        properties += '\t\t' + crnProp.data_type + ' ' + crnProp.key + ';\n';
    }
    let groupStruct = '\tstruct groupBy{ \n  \t\tstring hash;\n' + '  \t\tuint latestFact;\n' + '        uint size;\n\t' + '\t\tuint colSize;\n' +
        '  \t\tstring columns;\n' + '        uint timestamp;\n\t}\n';
    let gbView = '\tstruct gbView{ \n  \t\tstring viewDef;\n\t}\n'; // viewDef is a strigifiedJSON defining a view
    let viewMapping = '\tmapping(uint => gbView) public gbViews;\n\n';
    let groupMapping = '\tmapping(uint => groupBy) public groupBys;\n\n';
    properties += '\t\tuint timestamp;\n';
    let closeStruct = '\t}\n';
    let mapping = '\tmapping(uint =>' + fact_tbl.struct_Name + ') public facts;\n\n';
    let addParams = '';
    let addFact = '\tfunction addFact(';

    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            addParams += crnProp.data_type + ' ' + crnProp.key + ') ';
        } else {
            addParams += crnProp.data_type + ' ' + crnProp.key + ',';
        }
    }
    let retParams = 'public returns (';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            retParams += crnProp.data_type + ' ' + ', uint ID){\n';
        } else {
            retParams += crnProp.data_type + ' ' + ',';
        }
    }
    addFact = addFact + addParams + retParams;
    let setters = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        setters += '\t\tfacts[dataId].' + crnProp.key + '= ' + crnProp.key + ';\n';
    }
    setters += '\t\tfacts[dataId].timestamp = now;\n \t\tdataId += 1;\n';
    let retStmt = '\t\treturn (';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        retStmt += 'facts[dataId-1].' + crnProp.key + ',';
    }
    retStmt += 'dataId -1);\n\t}\n\n';

    let getParams = '';
    let getFact = '\tfunction getFact(uint id) public constant returns (';
    let retVals = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            getParams += crnProp.data_type + ' ' + crnProp.key + ', uint timestamp' + '){\n';
            retVals += 'facts[id].' + crnProp.key + ', facts[id].timestamp' + ');\n\t}\n\n';
        } else {
            getParams += crnProp.data_type + ' ' + crnProp.key + ',';
            retVals += 'facts[id].' + crnProp.key + ',';
        }
    }
    let retFact = '\t\treturn (' + retVals;

    let addView = '\tfunction addView(string definition) public returns(string viewAdded, uint viewID) { \n' +
        '\t\tgbViews[viewId].viewDef = definition;\n' +
        '\t\tviewId += 1;\n' +
        '\t\treturn (gbViews[viewId-1].viewDef, viewId-1);\n' +
        '\t}\n\n';

    let addGroupBy = '\tfunction addGroupBy(string hash, bytes32 category, uint latestFact, uint colSize, uint size, string columns) public returns(string groupAdded, uint groupID){\n' +
        '\t\tgroupBys[groupId].hash = hash;\n' +
        '\t\tgroupBys[groupId].timestamp = now;\n' +
        '\t\tgroupBys[groupId].latestFact = latestFact;\n' +
        '\t\tgroupBys[groupId].colSize = colSize;\n' +
        '\t\tgroupBys[groupId].size = size;\n' +
        '\t\tgroupBys[groupId].columns = columns;\n' +
        '\t\tif(category == COUNT_LITERAL){\n' +
        '\t\t\tlastCount  = groupID;\n' +
        '\t\t} else if(category == SUM_LITERAL){\n' +
        '\t\t\tlastSUM = groupID;\n' +
        '\t\t} else if(category == MIN_LITERAL){\n' +
        '\t\t\tlastMin = groupID;\n' +
        '\t\t} else if(category == MAX_LITERAL){\n' +
        '\t\t\tlastMax = groupID;\n' +
        '\t\t} else if(category == AVERAGE_LITERAL){\n' +
        '\t\t\tlastAverage = groupID;\n' +
        '\t\t}\n' +
        '\t\tgroupId += 1;\n' +
        '\t\treturn (groupBys[groupId-1].hash, groupId-1);\n' +
        '\t}\n\n';

    let getGroupBy = '\tfunction getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp, uint latFact, string cols, uint sz){\n' +
        '\t\treturn(groupBys[idGroup].hash, groupBys[idGroup].timestamp, groupBys[idGroup].latestFact, groupBys[idGroup].columns, groupBys[idGroup].size);\n' +
        '\t}\n\n';

    let getLatestGroupBy = '\tfunction getLatestGroupBy(bytes32 operation) public constant returns(string latestGroupBy, uint ts, uint latFactInGb, uint colSz, string gbCols){\n' +
        '\t\tif(groupId > 0){\n' +
        '\t\t\tif(operation == COUNT_LITERAL){\n' +
        '\t\t\t\tif(lastCount >= 0){\n' +
        '\t\t\t\t\treturn (groupBys[lastCount].hash, groupBys[lastCount].timestamp, groupBys[lastCount].latestFact, groupBys[lastCount].colSize, groupBys[lastCount].columns);\n' +
        '\t\t\t\t}\n' +
        '\t\t\t} else if (operation == SUM_LITERAL){\n' +
        '\t\t\t\tif(lastSUM >= 0){\n' +
        '\t\t\t\t\treturn (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp, groupBys[lastSUM].latestFact, groupBys[lastSUM].colSize, groupBys[lastSUM].columns);\n' +
        '\t\t\t\t}\n' +
        '\t\t\t} else if (operation == MIN_LITERAL){\n' +
        '\t\t\t\tif(lastMin >= 0){\n' +
        '\t\t\t\t\treturn (groupBys[lastMin].hash, groupBys[lastMin].timestamp, groupBys[lastMin].latestFact, groupBys[lastMin].colSize, groupBys[lastMin].columns);\n' +
        '\t\t\t\t}\n' +
        '\t\t\t} else if (operation == MAX_LITERAL){\n' +
        '\t\t\t\tif(lastMax >= 0){\n' +
        '\t\t\t\t\treturn (groupBys[lastMax].hash, groupBys[lastMax].timestamp, groupBys[lastMax].latestFact, groupBys[lastMax].colSize, groupBys[lastMax].columns);\n' +
        '\t\t\t\t}\n' +
        '\t\t\t} else if (operation == AVERAGE_LITERAL){\n' +
        '\t\t\t\tif(lastAverage >= 0){\n' +
        '\t\t\t\t\treturn (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp, groupBys[lastAverage].latestFact, groupBys[lastAverage].colSize, groupBys[lastAverage].columns);\n' +
        '\t\t\t\t}\n' +
        '\t\t\t}\n' +
        '\t\t}\n' +
        "\t\treturn (\"\",0,0,0,\"\");\n" +
        '\t}\n\n';

    let retValsLatest = '';
    let getParamsLatest = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            getParamsLatest += crnProp.data_type + ' ' + crnProp.key + '){\n';
            retValsLatest += 'facts[dataId-1].' + crnProp.key + ');\n\t';
        } else {
            getParamsLatest += crnProp.data_type + ' ' + crnProp.key + ',';
            retValsLatest += 'facts[dataId-1].' + crnProp.key + ',';
        }
    }
    let retFactLatest = '\t\t\treturn (' + retValsLatest;
    let emptyRetFactLatest = '';

    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            if (crnProp.data_type === 'string') {
                emptyRetFactLatest += "\"\"" + ");\n\t";
            } else {
                emptyRetFactLatest += '0' + ');\n\t';
            }
        } else {
            if (crnProp.data_type === 'string') {
                emptyRetFactLatest += "\"\"" + ', ';
            } else {
                emptyRetFactLatest += '0, ';
            }
        }
    }

    let getLatestFact = '\tfunction getLatestFact() public constant returns (' + getParamsLatest +
        '\t\tif(dataId > 0){\n' + retFactLatest +
        '\t} else {\n' +
        '\t\t\treturn (' + emptyRetFactLatest +
        '\t}\n' +
        '\t}\n\n';

    let getAllViews = '\tfunction getAllViews(uint viewID) public returns (string[] viewDefinitions){\n';
    let getAllViewsDec = '\t\tstring[] memory allViews = new string[](viewID);\n';
    let getViewsLoop = '\t\tfor(uint i =0; i < viewID; i++){\n' +
        '\t\t gbView storage crnView = gbViews[i];\n' +
        '\t\t allViews[i] = crnView.viewDef;\n' +
        '\t\t}\n' +
        '\t\treturn(allViews);\n' +
        '\t}\n';

    let getAllGBs = '\tfunction getAllGroupBys(uint groupById) public returns (string[] hashes, uint[] latFacts, uint[] columnSize, uint[] size,  string[] columns, uint[] gbTimestamp){\n';
    let getAllGBsDec = '\t\tstring[] memory allHashes = new string[](groupById);\n' + '\t\tuint[] memory allLatFact = new uint[](groupById);\n' + '\t\tuint[] memory allColSize = new uint[](groupById);\n' + '\t\tuint[] memory allSize = new uint[](groupById);\n' + '\t\tuint[] memory allTs = new uint[](groupById);\n' +
        '\t\tstring[] memory allColumns = new string[](groupById);\n';
    let getGBsLoop = '\t\tfor(uint i =0; i < groupById; i++){\n' +
        '\t\t groupBy storage crnGb = groupBys[i];\n' +
        '\t\t allHashes[i] = crnGb.hash;\n' +
        '\t\t allLatFact[i] = crnGb.latestFact;\n' +
        '\t\t allColSize[i] = crnGb.colSize;\n' +
        '\t\t allSize[i] = crnGb.size;\n' +
        '\t\t allColumns[i] = crnGb.columns;\n' +
        '\t\t allTs[i] = crnGb.timestamp;\n' +
        '\t\t}\n' +
        '\t\treturn(allHashes, allLatFact, allColSize, allSize, allColumns, allTs);\n' +
        '\t}\n';

    let getAllFacts = '\tfunction getAllFacts(uint id) public returns (';
    let getParamsAll = '';
    let retValsAll = '';
    let assignements = '';
    let retStmtAll = '';
    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length-1)) {
            getParamsAll += crnProp.data_type + '[] ' + crnProp.key + 's, uint[] timestamps' + '){\n';
            retValsAll += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](id);\n';
            retValsAll += '\t\tuint[] memory timestampss = new uint[](id);\n';
            assignements += '\t\t\t' + crnProp.key + 'ss[i] = fact.' + crnProp.key+';\n';
            assignements += '\t\t\t' + 'timestampss[i] = fact.timestamp;\n';
            assignements += '\t\t}\n';
            retStmtAll += crnProp.key + 'ss,';
            retStmtAll += 'timestampss);\n'
        } else {
            getParamsAll += crnProp.data_type + '[] ' + crnProp.key + 's,';
            retValsAll += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](id);\n';
            assignements += '\t\t\t' + crnProp.key + 'ss[i] = fact.' + crnProp.key+';\n';
            retStmtAll += crnProp.key + 'ss,';
        }
    }
    let loopLine = '\t\tfor(uint i =0; i < id; i++){\n';
    let firstLoopLine = '\t\t\t' + fact_tbl.struct_Name + ' storage fact = facts[i];\n';

    let getAllRet = '\t\treturn (';
    getAllRet += retStmtAll;
    loopLine += firstLoopLine + assignements + getAllRet + '\t}\n';

    getAllFacts = getAllFacts + getParamsAll + retValsAll + loopLine + '\n';

    let getFactFromTo = '\tfunction getFactsFromTo(uint from, uint to) public returns (';
    let getParamsFromTo = '';
    let retValsFromTo = '';
    let assignementsFromTo = '';
    let retStmtFromTo = '';
    let arrCounter = '\t\tuint j = 0;\n';
    let counterIncr = '\t\t\tj++;\n';

    for (let i = 0; i < fact_tbl.properties.length; i++) {
        let crnProp = fact_tbl.properties[i];
        if (i === (fact_tbl.properties.length - 1)) {
            getParamsFromTo += crnProp.data_type + '[] ' + crnProp.key +'sFromTo, uint[] timestampsFromTo' + '){\n';
            retValsFromTo += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](to - from);\n';
            retValsFromTo += '\t\tuint[] memory timestampss = new uint[](to - from);\n';
            assignementsFromTo += '\t\t\t' + crnProp.key + 'ss[j] = fact.' + crnProp.key+';\n';
            assignementsFromTo += '\t\t\t' + 'timestampss[j] = fact.timestamp;\n';
            assignementsFromTo += counterIncr;
            assignementsFromTo += '\t\t}\n';
            retStmtFromTo += crnProp.key + 'ss,';
            retStmtFromTo += 'timestampss);\n'
        } else {
            getParamsFromTo += crnProp.data_type + '[] ' + crnProp.key + 's,';
            retValsFromTo += '\t\t' + crnProp.data_type + '[] memory ' + crnProp.key + 'ss = new ' + crnProp.data_type + '[](to - from);\n';
            assignementsFromTo += '\t\t\t' + crnProp.key + 'ss[j] = fact.' + crnProp.key+';\n';
            assignementsFromTo += counterIncr;
            retStmtFromTo += crnProp.key + 'ss,';
        }
    }

    let loopLineFromTo = '\t\tfor(uint i = from; i < to; i++){\n';
    let firstLoopLineFromTo = '\t\t\t' + fact_tbl.struct_Name + ' storage fact = facts[j];\n';

    let getRetFromTo = '\t\treturn (';
    getRetFromTo += retStmtFromTo;
    loopLineFromTo += firstLoopLineFromTo + assignementsFromTo + getRetFromTo + '\t}\n';

    getFactFromTo = getFactFromTo + getParamsFromTo + retValsFromTo + arrCounter + loopLineFromTo;
    let addManyFacts = '\tfunction addFacts(string[] payloadsss) public returns (string, uint IDMany){\n' +
        '\t\tfor(uint i =0; i < payloadsss.length; i++){\n' +
        '\t\t\tfacts[dataId].payload= payloadsss[i];\n' +
        '\t\t\tfacts[dataId].timestamp = now;\n' +
        '\t\t\tdataId += 1;\n' +
        '\t\t}\n' +
        '\t\treturn (facts[dataId-1].payload,dataId -1);\n' +
        '\t}\n';

    let deleteGBById = '\tfunction deleteGBsById(uint[] gbIds) public returns (uint){\n';
    deleteGBById += '\t\tfor(uint i=0; i < gbIds.length; i++){\n';
    deleteGBById += '\t\t\tuint crnDelId = gbIds[i];\n';
    deleteGBById += '\t\t\tdelete groupBys[crnDelId];\n';
    deleteGBById += '\t\t}\n';
    deleteGBById += '\t\treturn (gbIds[gbIds.length - 1]);\n';
    deleteGBById += '\t}\n';

    contrPayload = firstLine + secondLine + thirdLine + fourthLine + sixthLine + fifthLine +
        constr + struct + properties + closeStruct + groupStruct + groupMapping +  mapping + gbView +
        viewMapping + addFact + setters + retStmt + getFact + getParams + retFact + addView + addGroupBy +
        getGroupBy + getLatestGroupBy + getAllViews + getAllViewsDec + getViewsLoop + getAllGBs + getAllGBsDec +
        getGBsLoop + getAllFacts + getFactFromTo + addManyFacts + deleteGBById + '\n}';
    return new Promise( function (resolve , reject) {
        fs.writeFile('contracts/' + fact_tbl.name + '.sol', contrPayload, function (err) {
            if (err) {
                console.log(err);
                return reject({msg: 'error'});
            }
            console.log('The file was saved!');
            let templ = {};
            if ('template' in fact_tbl) {
                templ = fact_tbl['template'];
            } else {
                templ = fact_tbl;
            }
            console.log(templ);
            return resolve({msg: 'OK', filename: fact_tbl.name, template: templ, createTable: createTable,tableName: tableName });
        });
    });
}

module.exports = {
    generateContract: generateContract
};
