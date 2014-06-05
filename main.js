/*!
 * Samsaara Inter Process Communication (Redis) Module
 * Copyright(c) 2014 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */

var debug = require('debug')('samsaara:ipcRedis');
var debugMessaging = require('debug')('samsaara:ipcRedis:messaging');
var debugProcesses = require('debug')('samsaara:ipcRedis:process');
var debugProcessCallBacks = require('debug')('samsaara:ipcRedis:callback');


var redis = require('redis');
var redisSub = redis.createClient(),
    redisPub = redis.createClient(),
    redisClient = redis.createClient();

var core,
    connectionController,
    communication,
    router,
    connections;

var Process,
    SymbolicConnection;

var routes = {},
    routeList = {};  

var processes = {};


// main module object definition (what is exported to samsaara middlware load)

var ipcRedis = {

  name: "ipc",

  moduleExports: {
    addRoute: addRoute,
    removeRoute: removeRoute,
    publishToRoute: publishToRoute,
    sendCallBackList: sendCallBackList,
    publish: publish,
    subscribe: subscribe,
    unsubscribe: unsubscribe,   
    subscribePattern: subscribePattern,
    unsubscribePattern: unsubscribePattern,
    routes: routes,
    store: redisClient,
    process: process,
    generateSymbolic: generateSymbolic,
    use: use
  },

  main: {},

  constructors : {
    SymbolicConnection: SymbolicConnection,
    Process: Process
  },

  connectionInitialization: {
    ipc: connectionInitialzation
  },

  connectionClose: {
    ipc: connectionClosing        
  },

  messageRoutes: {
    OWN: ipcMessageRoute
  }
};


// the root interface loaded by require. Options are pass in as options here.

function main(opts){
  return initialize;
}


// samsaara will call this method when it's ready to load it into its middleware stack
// return your module's definition object. 

function initialize(samsaaraCore){

  core = samsaaraCore;

  connectionController = samsaaraCore.connectionController;
  communication = samsaaraCore.communication;
  router = samsaaraCore.router;
  connections = connectionController.connections;

  symbolic = require('./symbolic');
  SymbolicConnection = symbolic.initialize(samsaaraCore);
  Process = require('./process').initialize(samsaaraCore, processes, publish);

  core.samsaara.createNamespace("interprocess", {
    requestDataForSymbolic: requestDataForSymbolic,
    createSymbolicConnection: createSymbolicConnection
  });


  // handles a new process on the system
  addRoute("newProcess", "PRC:NEW", handleNewProcess);
  addRoute("deleteProcess", "PRC:DEL", handleDeadProcess);

  ensureUniqueCoreUuid(core.uuid, function(){

    publish("PRC:NEW", core.uuid);

    // handles messages that are redirected to this process
    addRoute("process", "PRC:"+core.uuid+":FWD", handleForwardedMessage);

    // handles a list of connections that we are expecting a callback from
    addRoute("callBackList", "PRC:"+core.uuid+":CBL", handleCallBackList);

    // creates a new symbolic connection
    addRoute("deleteSymbolicConnection", "PRC:"+core.uuid+":SYMDEL", handleDeleteSymbolicConnection);

    // handles a generic process function execution request
    addRoute("executeProcessFunction", "PRC:"+core.uuid+":EXEC", handleInterprocessExecute);

    // handles a generic process function execution request
    addRoute("executeProcessCallBack", "PRC:"+core.uuid+":IPCCB", handleIPCCallBack);
    

    // get a list of all processes
    redisClient.hkeys("samsaara:processes", function (err, replies){

      debugProcesses("samsaara Process List", replies);

      for (var i = 0; i < replies.length; i++) {          
        if(replies[i] !== core.uuid){
          processes[replies[i]] = new Process(replies[i]);
        }
      }
    });
  });


  return ipcRedis;
}



// Middleware middleware :)
// load additional handlers for when new Symbolic Connections are created.

function use(middleware){
  if(middleware.symbolicConnectionInitialization){
    symbolic.initializationMethods.push(middleware.symbolicConnectionInitialization);
  }
  if(middleware.symbolicConnectionClosing){
    symbolic.closingMethods.push(middleware.symbolicConnectionClosing);
  }
}


// Route Creation/Removal Methods

// add a new route with a friendly name

function addRoute(routeName, channel, func){
  routes[channel] = func;
  routeList[routeName] = channel;
  subscribe(channel);
}

// remove a route by friendly name

function removeRoute(routeName){
  unsubscribe(routeList[routeName]);
  delete routes[routeList[routeName]];
  delete routeList[routeName];
}

// less useful, publish to a route by friendly name.

function publishToRoute(routeName, message){
  publish(routes[routeList[routeName]], message);
}


// Base Pub/Sub

function publish(channel, message){
  debug("PUBLISH", channel, message);
  redisPub.publish(channel, message); 
}

function subscribe(channel){
  redisSub.subscribe(channel);
}

function unsubscribe(channel){
  redisSub.unsubscribe(channel); 
}

function subscribePattern(pattern){
  redisSub.psubscribe(pattern); 
}

function unsubscribePattern(pattern){
  redisSub.punsubscribe(pattern); 
}


// 
// Route Methods (for IPC)
// messaging/routing. might need to split this off into its own submodule
// 

redisSub.on("message", function (channel, message) {
  debugMessaging("New IPC Message", channel, message);
  switchMessages(channel, message);
});


// The use of "headers" should be documented. Basically, headers should be used when forwarding messages
// to prevent the need to prematurely parse JSON strings, only to have to repackage the JSON object again
// anyway. So it's more efficient to parse a header and analyze whether or not to receive it or to forward it.
// most of the "handlers" here handle forwarded messages that have modified headers.
// An exception where one can use headers may be for messages that are simple, predictable in format and only hold
// string or numeric values. This information can be passed through headers alone.

// Main IPC Router Internal

function switchMessages(channel, message){
  routes[channel](channel, message);
}


// Adds a new peer process instance to this process for reference. Usually sent by a new process joining the network.
//
// channel here is a STRING in the form: "PRC:NEW"
// message here is a STRING of the form: "peerUuid"

function handleNewProcess(channel, peerUuid){
  debugProcesses("New Process", peerUuid);
  processes[peerUuid] = new Process(peerUuid);
}


function handleDeadProcess(channel, peerUuid){    
  delete processes[peerUuid];
}

// EXPOSED to ipc module retrieves a peer process instance.
// ie. ipc.process(processID).execute()...

function process(processUuid){
  debugProcesses("Process Retrieval", processes, processUuid);
  return processes[processUuid];
}


// This method is executed when a message from a connection is forwarded from another process
// to this process, usually in response to a callback request that was sent from this process
// to connections on other processes, we may create a separate thing for callBacks though.
// 
// channel here is a STRING in the form: "PRC:ProcessUUID:FWD"
// message here is a STRING of the form: "FRM:connectionID::{samsaaraJSONFunctionCall}"

function handleForwardedMessage(channel, message){

  debugMessaging("Handle Forwarded Message", core.uuid, channel, message);

  var index = message.indexOf("::");
  var header = message.substring(0, index);
  var headerSplit = header.split(":");

  var connMessage = message.slice(2+index-message.length);
  var connID = headerSplit[headerSplit.indexOf("FRM")+1];

  var messageObj = parseJSON(connMessage);

  debug("Process Message", core.uuid, headerSplit, connID, messageObj);

  var connection = connections[connID] || {id: connID};

  if(messageObj !== undefined && messageObj.func){
    communication.executeFunction(connection, connection, messageObj);
  }
}

function splitHeaderMessage(rawmessage){
  var index = rawmessage.indexOf("::");
  var header = rawmessage.substring(0, index);
  return { header: header.split(":"), message: rawmessage.slice(2+index-rawmessage.length)};
}


// Used to handle a list of connections that are expected to return values
// to a callBack on this process.
//
// channel here is a STRING in the form: "PRC:ProcessUUID:CBL"
// message here is a STRING of the form: "callBackID:connectionID:connectionID:..:connectionID"

function handleCallBackList(channel, message){

  var callBackListSplit = message.split(":");
  var callBackID = callBackListSplit.shift();

  // debug("Adding CallBack Connections", callBackID, callBackListSplit, communication.incomingCallBacks);
  
  communication.incomingCallBacks[callBackID].addConnections(callBackListSplit || []);
}



//
// SYMBOLIC CONNECTION
//

// Local API
// generates a symbolic connection of the connection ID. Contacts connection owner
// and gets connectionData needed to build a *decent* representation of the connection locally.

function generateSymbolic(connID, callBack){

  redisClient.hget("samsaara:connectionOwners", connID, function (err, ownerID){
    if(ownerID === null){
      if(typeof callBack === "function") callBack(err, null);
    }
    else{
      process(ownerID).execute("requestDataForSymbolic", connID, core.uuid, function (err, symbolicConnectionData){
        if(err){
          if(typeof callBack === "function") callBack(err, null);
        }
        else{
          connections[connID] = new SymbolicConnection(ownerID, connID, symbolicConnectionData);
          if(typeof callBack === "function") callBack(err, symbolicConnection);
        }          
      });
    }
  });
}


// IPC Exposed API
// InterProcess exposed methods (methods other processes can execute on this process) (in namespace["interprocess"])

function requestDataForSymbolic(connID, processID, callBack){
  if(connections[connID]){
    connections[connID].symbolicOwners[processID] = true;
    if(typeof callBack === "function") callBack(null, connections[connID].connectionData);
  }
  else{
    if(typeof callBack === "function") callBack(new Error("Connection:" + connID + " Does not exist on process:"+core.uuid), null);
  }
}


// IPC Exposed API
// called by another process, that creates a symbolic representation of a connection
// with the supplied symbolicData on this process/machine.

function createSymbolicConnection(connID, symbolicData, callBack){
  if(!connections[connID]){
    connections[connID] = new SymbolicConnection(ownerID, connID, symbolicConnectionData);      
    if(typeof callBack === "function") callBack(null);
  }
  else{
    if(typeof callBack === "function") callBack("ConnectionID/Symbolic Connection ID exists on process " + core.uuid);
  }
}



// Adds a new "symbolic connection" to this process.
// channel here is a STRING in the form: "PRC:ProcessUUID:SYMNEW"
// message here is a STRING of the form: "{SymbolicData}"

// function handleNewSymbolicConnection(channel, message){    

//   var symbolicData = parseJSON(message);
//   var symbolicConnID = symbolicData.nativeID;
//   connections[symbolicConnID] = new SymbolicConnection(symbolicData);

// }


// channel here is a STRING in the form: "PRC:processID:SYMDEL"
// message here is a STRING of the form: "connectionID"

function handleDeleteSymbolicConnection(channel, connID){

  if(connections[connID] !== undefined){
    delete connections[connID];
  }
}

// executes a method call from another process on this process. Taps into the core communication function execution method
// with a custom callBackGenerator.
//
// channel here is a STRING in the form: "PRC:processUUID:EXEC"
// message here is a STRING of the form: {owner: processUuid, func: functionName, ns: "interprocess", args: argsArray, callBack: callBackID}

function handleInterprocessExecute(channel, message){   

  var messageObj = parseJSON(message)[1];
  messageObj.ns = "interprocess";
  var executor = {id: messageObj.owner};

  // debugProcesses("Handle New Execute", message, executor);

  communication.executeFunction(executor, executor, messageObj, createIPCCallBack);
}



//
// CALLBACKS
// 

// channel here is a STRING in the form: "PRC:processUUID:IPCCB"
// message here is a STRING of the form: {owner: processUuid, func: functionName, ns: "interprocess", args: argsArray, callBack: callBackID}

function handleIPCCallBack(channel, message){

  // debugProcessCallBacks("Handle IPC CallBack", channel, message);

  var messageObj = parseJSON(message);
  var executor = {id: messageObj.sender}; // need to modify communication.callItBack to accept more than just connections as callback executors
  communication.executeFunction(executor, executor, messageObj, createIPCCallBack);
}

function createIPCCallBack(id, sender, owner){

  // debugProcessCallBacks("Creating IPC Callback");

  var theCallBack = function(){
    var args = Array.prototype.slice.call(arguments);
    publish("PRC:"+owner+":IPCCB", JSON.stringify({ns: "internal", func: "callItBack", args: ["samsaara.self", id, args], sender: core.uuid}) );
    delete communication.outgoingCallBacks[id];
  };

  theCallBack.id = id;

  return theCallBack;
}


function sendCallBackList(processID, callBackID, callBackList){
  // debug("ipcRedis", processUuid, "SENDING CALLBACK LIST ", processID, callBackID, callBackList);
  publish("PRC:"+processID+":CBL", callBackID+callBackList);
}

function sendCallBackExecute(processID, callBackID, message){
  publish("PRC:"+processID+":CBX", callBackID+":"+callBackList);
}


//
// CONNECTION MESSAGES
// 

// symbolic connection handlers

function handleSymbolicMessage(channel, message){

  // channel here is a STRING in the form: "SYM:symbolicConnID:MSG"
  // message here is a STRING of the form: "{samsaaraJSONFunctionCall}"

  var symbolicConnID = channel.split(":")[1];
  var symbolicConnection = connections[symbolicConnID] || {id: symbolicConnID};
  var messageObj = parseJSON(message);
  if(messageObj !== undefined){
    communication.executeFunction(symbolicConnection, symbolicConnection, message);
  }    
}


// native connection message handlers  
// channel here is a STRING in the form: "NTV:connID:MSG"
// message here is a STRING of the form: "{samsaaraJSONFunctionCall}"

function handleMessageToNativeConnection(channel, message){

  // !! need to test this, individual subscriptions to native connections, vs. this single router?

  var connID = channel.split(":")[1];

  if(connections[connID] !== undefined){
    connections[connID].write(message);
  }
}



// Connection Initialization/Close Methods
// Called for all new connections

function connectionInitialzation(opts, connection, attributes){

  var connID = connection.id;

  debug("Initializing IPC Subscription!!!", core.uuid, opts.groups, connID);

  attributes.force("ipc");

  connection.symbolicOwners = {};
  redisClient.incr("totalCurrentCount");
  addRoute("ntvMsg"+connID, "NTV:"+connID+":MSG", handleMessageToNativeConnection);

  redisClient.hset("samsaara:connectionOwners", connID, core.uuid, function (err, reply){
    attributes.initialized(null, "ipc");
  });    
}


function connectionClosing(connection){

  var connID = connection.id;

  redisClient.decr("totalCurrentCount");

  for(var owner in connection.symbolicOwners){
    ipc.publish("PRC:"+owner+":SYMDEL", connID);
  }

  removeRoute("ntvMsg"+connID);

  redisClient.hdel("samsaara:connectionOwners", connID, function (err, reply){

  });
}


// Message Router: Overwrites the core routeMessage method in the samsaara router

function ipcMessageRoute(connection, headerbits, message){

  var owner = headerbits[1];

  if(owner === core.uuid){

    var messageObj = parseJSON(message);

    if(messageObj !== undefined && messageObj.func !== undefined){
      messageObj.sender = connection.id;
      communication.executeFunction(connection, connection, messageObj);
    }
  }
  else{       
    sendClientMessageToProcess(owner, connection.id, message);
  }
}

// outgoing messages/objects

function sendClientMessageToProcess(processID, connID, message){
  // debug("Publishing to", "PRC:"+processID+":FWD". message );
  publish("PRC:"+processID+":FWD", "FRM:"+connID+"::"+message);
}


// ensure that the uuid for this process is unique in the entire system, if not, generate a new one and repeat

function ensureUniqueCoreUuid(uuid, callBack){
  redisClient.hsetnx("samsaara:processes", uuid, "node", function (err, reply){

    debugProcesses("Unique UUID for Samsaara Process", uuid, err, reply);

    if(err) throw err;

    if(reply === 0){
      core.uuid = makeIdAlphaNumerical(8);
      ensureUniqueCoreUuid(core.uuid);        
    }
    else{
      if(typeof callBack === "function") callBack();
    }
  });
}




// Helper Methods

function parseJSON(jsonString){
  var parsed;

  try{
    parsed = JSON.parse(jsonString);      
  }
  catch(e){
    debug("Message Error: Invalid JSON", jsonString, e);
  }

  return parsed;
}


function makeIdAlphaNumerical(idLength){
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for( var i=0; i < idLength; i++ ){
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}



module.exports = exports = main;


