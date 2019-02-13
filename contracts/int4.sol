pragma solidity ^0.4.0;

contract int4 { 
	uint public dataId;
	uint public groupId;

	constructor() {
		dataId = 0;
		groupId = 0;
	}

	struct int_4{ 
		uint key_1;
		uint key_2;
		uint key_3;
		uint key_4;
		uint timestamp;
	}
	struct groupBy{ 
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>int_4) public facts;

	function addFact(uint key_1,uint key_2,uint key_3,uint key_4) public returns (uint ,uint ,uint ,uint , uint ID){
		facts[dataId].key_1= key_1;
		facts[dataId].key_2= key_2;
		facts[dataId].key_3= key_3;
		facts[dataId].key_4= key_4;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].key_1,facts[dataId-1].key_2,facts[dataId-1].key_3,facts[dataId-1].key_4,dataId -1);
	}

	function getFact(uint id) public constant returns (uint key_1,uint key_2,uint key_3,uint key_4, uint timestamp){
		return (facts[id].key_1,facts[id].key_2,facts[id].key_3,facts[id].key_4, facts[id].timestamp);
	}

	function addGroupBy(string hash) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].hash = hash;
    		groupBys[groupId].timestamp = now;
    		groupId += 1;
    		return (groupBys[groupId-1].hash, groupId-1);
    	}

	function getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){
    		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp);
    	}

	function getLatestGroupBy() public constant returns(string latestGroupBy, uint ts){
		if(groupId > 0){
			return (groupBys[groupId-1].hash, groupBys[groupId-1].timestamp);
		} else {
			return ("",0);
		}
	}


}