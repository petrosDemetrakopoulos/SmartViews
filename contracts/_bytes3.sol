pragma solidity ^0.4.0;

contract _bytes3 { 
	uint public dataId;
	uint public groupId;

	constructor() {
		dataId = 0;
		groupId = 0;
	}

	struct bytes_3{ 
		bytes32 key_1;
		bytes32 key_2;
		bytes32 key_3;
		uint timestamp;
	}
	struct groupBy{ 
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>bytes_3) public facts;

	function addFact(bytes32 key_1,bytes32 key_2,bytes32 key_3) public returns (bytes32 ,bytes32 ,bytes32 , uint ID){
		facts[dataId].key_1= key_1;
		facts[dataId].key_2= key_2;
		facts[dataId].key_3= key_3;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].key_1,facts[dataId-1].key_2,facts[dataId-1].key_3,dataId -1);
	}

	function getFact(uint id) public constant returns (bytes32 key_1,bytes32 key_2,bytes32 key_3, uint timestamp){
		return (facts[id].key_1,facts[id].key_2,facts[id].key_3, facts[id].timestamp);
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