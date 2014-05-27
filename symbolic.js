/*!
 * samsaaraSocks - SymbolicConnection Constructor
 * Copyright(c) 2013 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */


var debug = require('debug')('samsaara:ipcRedis:symbolic');

var samsaara,
    ipc;


function initialize(samsaaraCore){
  samsaara = samsaaraCore;
  ipc = samsaaraCore.ipc;
}

function SymbolicConnection(ownerID, connID, symbolicConnectionData){
  this.id = connID;
  this.connectionClass = "symbolic";
  this.owner = ownerID;
  this.connectionData = symbolicConnectionData;
}

SymbolicConnection.prototype.write = function(message){
  debug(process.pid.toString(), "SYMBOLIC write on", "SYMBOLIC CONNECTION PUBLISHING: Owner:", this.owner, this.nativeID);  
  ipc.publish("NTV:"+this.nativeID+":MSG", message);
};

SymbolicConnection.prototype.updateDataAttribute = function(attributeName, value) {
  this.connectionData[attributeName] = value;
};

SymbolicConnection.prototype.closeConnection = function(message){
  var connID = this.id;
  samsaara.emit("symbolicDisconnect", this);
  delete connections[connID];
};

exports = module.exports = {
  initialize: initialize,
  SymbolicConnection: SymbolicConnection
};