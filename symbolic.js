/*!
 * samsaaraSocks - SymbolicConnection Constructor
 * Copyright(c) 2013 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */


var debug = require('debug')('samsaara:ipcRedis:symbolic');

var core, samsaara;

var publish;

var initializationMethods = [],
    closingMethods = [];


function initialize(samsaaraCore, publishMethod){
  core = samsaaraCore;
  samsaara = samsaaraCore.samsaara;

  publish = publishMethod;

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


// creates a namespace object that holds an execute method with the namespace as a closure..

SymbolicConnection.prototype.nameSpace = function(nameSpaceName){

  var connection = this;

  return {
    execute: function execute(){
      var packet = {ns:nameSpaceName, func: arguments[0], args: []};
      executeOnConnection(connection, packet, arguments);
    }
  };
};


// Method to execute methods on the client.

SymbolicConnection.prototype.execute = function(){

  var connection = this;
  var packet = {func: arguments[0], args: []};
  executeOnConnection(connection, packet, arguments);
};


function executeOnConnection(connection, packet, args){

  communication.processPacket(0, packet, args, function (incomingCallBack, packetReady){
    if(incomingCallBack !== null){
      incomingCallBack.addConnection(connection.id);
    }
    connection.write(packetReady); // will send directly or via symbolic
  });
}

// Method to execute methods on the client.

SymbolicConnection.prototype.executeRaw = function(packet, callback){

  var connection = this;

  if(typeof callback === "function"){
    communication.makeCallBack(0, packet, callback, function (incomingCallBack, packetReady){
      incomingCallBack.addConnection(connection.id);
      connection.write(packetReady); // will send directly or via symbolic
    });
  }
  else{
    var sendString;
    try{
      sendString = JSON.stringify([core.uuid, packet]);
    }
    catch(e){
      console.log("ERROR SENDING PACKET", core.uuid, packet);
    }

    connection.write( sendString );
  }

};


function processPacket(packet, args){

  for (var i = 1; i < args.length-1; i++){
    packet.args.push(args[i]);
  }

  if(typeof args[args.length-1] === "function"){
    packet = core.makeCallBack(packet, args[args.length-1]);
  }
  else{
    packet.args.push(args[args.length-1]);
  }

  return packet;
}


SymbolicConnection.prototype.write = function(message){
  debug(process.pid.toString(), "SYMBOLIC write on", "SYMBOLIC CONNECTION PUBLISHING: Owner:", this.owner, this.id);  
  publish("NTV:"+this.id+":MSG", message);
};

SymbolicConnection.prototype.updateDataAttribute = function(attributeName, value) {
  this.connectionData[attributeName] = value;
};

SymbolicConnection.prototype.closeConnection = function(message){
  var connID = this.id;
  samsaara.emit("symbolicConnectionDisconnect", this);

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