/*!
 * samsaaraSocks - SymbolicConnection Constructor
 * Copyright(c) 2013 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */


var debug = require('debug')('samsaara:ipcRedis:symbolic');

var samsaara,
    ipc;

var initializationMethods = [],
    closingMethods = [];


function initialize(samsaaraCore){
  samsaara = samsaaraCore;
  ipc = samsaaraCore.ipc;

  return SymbolicConnection;
}





function SymbolicConnection(ownerID, connID, symbolicConnectionData){
  this.id = connID;
  this.connectionClass = "symbolic";
  this.owner = ownerID;
  this.connectionData = symbolicConnectionData;

  for(var i=0; i < initializationMethods.length; i++){
    initializationMethods[i](this);
  }

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

  for(var i=0; i < closingMethods.length; i++){
    closingMethods[i](this);
  }

  delete connections[connID];
};

exports = module.exports = {
  initialize: initialize,
  SymbolicConnection: SymbolicConnection,
  initializationMethods: initializationMethods,
  closingMethods: closingMethods
};