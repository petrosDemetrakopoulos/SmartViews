pragma solidity ^0.4.24;
pragma experimental ABIEncoderV2;
contract Sales { 
	uint public dataId;
	uint public groupId;

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
	struct Sale{ 
		string payload;
		uint timestamp;
	}
	struct groupBy{ 
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>Sale) public facts;

	function addFact(string payload) public returns (string , uint ID){
		facts[dataId].payload= payload;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].payload,dataId -1);
	}

	function getFact(uint id) public constant returns (string payload, uint timestamp){
		return (facts[id].payload, facts[id].timestamp);
	}

	function addGroupBy(string hash, bytes32 category) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].hash = hash;
    		groupBys[groupId].timestamp = now;
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

	function getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){
    		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp);
    	}

function getLatestGroupBy(bytes32 operation) public constant returns(string latestGroupBy, uint ts){
		if(groupId > 0){
			if(operation == COUNT_LITERAL){
				if(lastCount >= 0){
					return (groupBys[lastCount].hash, groupBys[lastCount].timestamp);
				}
			} else if (operation == SUM_LITERAL){
				if(lastSUM >= 0){
					return (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp);
				}
			} else if (operation == MIN_LITERAL){
				if(lastMin >= 0){
					return (groupBys[lastMin].hash, groupBys[lastMin].timestamp);
				}
			} else if (operation == MAX_LITERAL){
				if(lastMax >= 0){
					return (groupBys[lastMax].hash, groupBys[lastMax].timestamp);
				}
			} else if (operation == AVERAGE_LITERAL){
				if(lastAverage >= 0){
					return (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp);
				}
			}
		}
			return ("",0);
	}

	function getAllFacts(uint id) public returns (string[] payloads, uint[] timestamps){
		string[] memory payloadss = new string[](id);
		uint[] memory timestampss = new uint[](id);
		for(uint i =0; i < id; i++){
			Sale storage fact = facts[i];
			payloadss[i] = fact.payload;
			timestampss[i] = fact.timestamp;
		}
		return (payloadss,timestampss);
	}

}