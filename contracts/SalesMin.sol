pragma solidity ^0.4.24;
pragma experimental ABIEncoderV2;
contract SalesMin { 
	uint public dataId;
	uint public groupId;

	uint public viewId;

	uint public lastCount;
	uint public lastSUM;
	uint public lastMin;
	uint public lastMax;
	uint public lastAverage;
	bytes32 MIN_LITERAL = "MIN";
	bytes32 MAX_LITERAL = "MAX";
	bytes32 AVERAGE_LITERAL = "AVERAGE";
	bytes32 COUNT_LITERAL = "COUNT";
	bytes32 SUM_LITERAL = "SUM";
	constructor() {
		dataId = 0;
		groupId = 0;
		lastCount = 0;
		lastSUM = 0;
		lastMin = 0;
		lastMax = 0;
		lastAverage = 0;
	}
	struct Salemin{ 
		string payload;
		uint timestamp;
	}
	struct groupBy{ 
  		string hash;
  		uint latestFact;
  		uint colSize;
  		string columns;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>Salemin) public facts;

	struct gbView{ 
  		string viewDef;
}
	mapping(uint => gbView) public gbViews;

	function addFact(string payload) public returns (string , uint ID){
		facts[dataId].payload= payload;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].payload,dataId -1);
	}

	function getFact(uint id) public constant returns (string payload, uint timestamp){
		return (facts[id].payload, facts[id].timestamp);
	}

	function addView(string definition) public returns(string viewAdded, uint viewID) { 
    		gbViews[viewId].viewDef = definition;
    		viewId += 1;
    		return (gbViews[viewId-1].viewDef, viewId-1);
    	}

	function addGroupBy(string hash, bytes32 category, uint latestFact, uint colSize, string columns) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].hash = hash;
    		groupBys[groupId].timestamp = now;
    		groupBys[groupId].latestFact = latestFact;
    		groupBys[groupId].colSize = colSize;
    		groupBys[groupId].columns = columns;
			if(category == COUNT_LITERAL){
				lastCount  = groupID;
			} else if(category == SUM_LITERAL){
				lastSUM = groupID;
			} else if(category == MIN_LITERAL){
				lastMin = groupID;
			} else if(category == MAX_LITERAL){
				lastMax = groupID;
			} else if(category == AVERAGE_LITERAL){
				lastAverage = groupID;
			}
    		groupId += 1;
    		return (groupBys[groupId-1].hash, groupId-1);
    	}

	function getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp, uint latFact, string cols){
    		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp, groupBys[idGroup].latestFact, groupBys[idGroup].columns);
    	}

function getLatestGroupBy(bytes32 operation) public constant returns(string latestGroupBy, uint ts, uint latFactInGb, uint colSz, string gbCols){
		if(groupId > 0){
			if(operation == COUNT_LITERAL){
				if(lastCount >= 0){
					return (groupBys[lastCount].hash, groupBys[lastCount].timestamp, groupBys[lastCount].latestFact, groupBys[lastCount].colSize, groupBys[lastCount].columns);
				}
			} else if (operation == SUM_LITERAL){
				if(lastSUM >= 0){
					return (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp, groupBys[lastSUM].latestFact, groupBys[lastSUM].colSize, groupBys[lastSUM].columns);
				}
			} else if (operation == MIN_LITERAL){
				if(lastMin >= 0){
					return (groupBys[lastMin].hash, groupBys[lastMin].timestamp, groupBys[lastMin].latestFact, groupBys[lastMin].colSize, groupBys[lastMin].columns);
				}
			} else if (operation == MAX_LITERAL){
				if(lastMax >= 0){
					return (groupBys[lastMax].hash, groupBys[lastMax].timestamp, groupBys[lastMax].latestFact, groupBys[lastMax].colSize, groupBys[lastMax].columns);
				}
			} else if (operation == AVERAGE_LITERAL){
				if(lastAverage >= 0){
					return (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp, groupBys[lastAverage].latestFact, groupBys[lastAverage].colSize, groupBys[lastAverage].columns);
				}
			}
		}
			return ("",0,0,0,"");
	}

	function getAllViews(uint viewID) public returns (string[] viewDefinitions){
		string[] memory allViews = new string[](viewID);
		for(uint i =0; i < viewID; i++){
		 gbView storage crnView = gbViews[i];
		 allViews[i] = crnView.viewDef;
		}
		return(allViews);
	}
	function getAllGroupBys(uint groupById) public returns (string[] hashes, uint[] latFacts, uint[] columnSize, string[] columns, uint[] gbTimestamp){
		string[] memory allHashes = new string[](groupById);
		uint[] memory allLatFact = new uint[](groupById);
		uint[] memory allColSize = new uint[](groupById);
		uint[] memory allTs = new uint[](groupById);
		string[] memory allColumns = new string[](groupById);
		for(uint i =0; i < groupById; i++){
		 groupBy storage crnGb = groupBys[i];
		 allHashes[i] = crnGb.hash;
		 allLatFact[i] = crnGb.latestFact;
		 allColSize[i] = crnGb.colSize;
		 allColumns[i] = crnGb.columns;
		 allTs[i] = crnGb.timestamp;
		}
		return(allHashes, allLatFact, allColSize, allColumns, allTs);
	}
	function getAllFacts(uint id) public returns (string[] payloads, uint[] timestamps){
		string[] memory payloadss = new string[](id);
		uint[] memory timestampss = new uint[](id);
		for(uint i =0; i < id; i++){
			Salemin storage fact = facts[i];
			payloadss[i] = fact.payload;
			timestampss[i] = fact.timestamp;
		}
		return (payloadss,timestampss);
	}

	function getFactsFromTo(uint from, uint to) public returns (string[] payloadsFromTo, uint[] timestampsFromTo){
		string[] memory payloadss = new string[](to - from);
		uint[] memory timestampss = new uint[](to - from);
			uint j = 0;
		for(uint i = from; i < to; i++){
			Salemin storage fact = facts[j];
			payloadss[j] = fact.payload;
			timestampss[j] = fact.timestamp;
			j++;
		}
		return (payloadss,timestampss);
	}
function addFacts(string[] payloadsss) public returns (string, uint IDMany){
		for(uint i =0; i < payloadsss.length; i++){
			facts[dataId].payload= payloadsss[i];
			facts[dataId].timestamp = now;
			dataId += 1;
		}
		return (facts[dataId-1].payload,dataId -1);
	}
}