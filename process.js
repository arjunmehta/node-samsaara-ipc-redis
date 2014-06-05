/*!
 * samsaaraSocks - Context Constructor
 * Copyright(c) 2013 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */


var core, 
    samsaara,
    communication;

var processes,
    publish;


function initialize(samsaaraCore, processesObj, publishMethod){

  core = samsaaraCore;
  samsaara = samsaaraCore.samsaara;
  communication = samsaaraCore.communication;  

  processes = processesObj;
  publish = publishMethod;
 
  return Process;
}


function Process(processUuid){
  this.id = this.processUuid = processUuid;
}


Process.prototype.execute = function(){

  var process = this;

  var packet = {owner: core.uuid, ns:"interprocess", func: arguments[0], args: []};

  for (var i = 1; i < arguments.length-1; i++){
    packet.args.push(arguments[i]);
  }

  if(typeof arguments[arguments.length-1] === "function"){
    communication.makeCallBack(0, packet, arguments[arguments.length-1], function (incomingCallBack, packetReady){
      publish("PRC:"+process.id+":EXEC", packetReady);
      incomingCallBack.addConnection(process.id);
    });
  }
  else{
    packet.args.push(arguments[arguments.length-1]);
    publish("PRC:"+process.id+":EXEC", JSON.stringify(packet));
  }
};


Process.prototype.createSymbolic = function(connection, callBack){

  var process = this;

  this.execute("createSymbolicConnection", connection.id, core.uuid, connection.connectionData, function (err){
    if(!err){
      connection.symbolicOwners[process.id] = true;
      if(typeof callBack === "function") callBack(err);
    }
    else{
      if(typeof callBack === "function") callBack(err);
    }
  });
};



module.exports = exports = {
  initialize: initialize,
  Process: Process
};
