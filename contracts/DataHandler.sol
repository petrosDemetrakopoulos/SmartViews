pragma solidity ^0.4.0;

contract DataHandler {
	uint public dataId;
    uint public groupId;

	 constructor() {
		dataId = 0;

	}

	struct Fact{
		uint productId;
		uint quantity;
		uint customerId;
		uint timestamp; //seconds since 1970
	}

	struct groupBy{
        string groups;
        uint timestamp;
	}

	mapping(uint => Fact) public facts;
	mapping(uint => groupBy) public groupBys;

	function addFact(uint prodId, uint quant, uint customer) public returns (uint prod, uint quantity, uint cutomerId, uint ID){
		facts[dataId].productId = prodId;
		facts[dataId].quantity = quant;
		facts[dataId].customerId = customer;
		facts[dataId].timestamp = now;
		dataId += 1;
		return (facts[dataId-1].productId,facts[dataId-1].quantity, facts[dataId-1].customerId, dataId -1);
	}

	function addGroupBy(string group) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].groups = group;
    		groupBys[groupId].timestamp = now;
    		groupId += 1;
    		return (groupBys[groupId-1].groups, groupId-1);
    	}

	function getFact(uint id) public constant returns (uint prodId, uint quant, uint customer){
		return(facts[id].productId, facts[id].quantity, facts[id].customerId);
	}

	function getGroupBy(uint idGroup) public constant returns (string groupByID){
    		return(groupBys[idGroup].groups);
    	}

}