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

function ipcRedis(options){

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

  var redisSub = redis.createClient(),
      redisPub = redis.createClient(),
      redisClient = redis.createClient();

  redisSub.on("message", function (channel, message) {
    debugMessaging("New IPC Message", channel, message);
    switchMessages(channel, message);
  });


  // retrieves a peer process instance.

  function process(processUuid){
    return processes[processUuid];
  }


  // Foundation Methods

  function addRoute(routeName, channel, func){
    routes[channel] = func;
    routeList[routeName] = channel;
    subscribe(channel);
  }

  function removeRoute(routeName){
    unsubscribe(routeList[routeName]);
    delete routes[routeList[routeName]];
    delete routeList[routeName];
  }

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

    // var testMessage = core.uuid + " to " + peerUuid + " sent";
    // debugProcesses("Testing Process-Process Communication", testMessage);

    // process(peerUuid).execute("testInterprocess", testMessage, function(message){
    //   debugProcessCallBacks("Testing Process-Process CallBack", testMessage, "CallBack:", message);
    // });
  }

  // function testInterprocess(message, callBack){
  //   debugProcesses("Testing Process-Process Communication", "Received and Executed testInterprocess", message, this.id);
  //   if(typeof callBack === "function") callBack(core.uuid + " to " + this.id + " sent");
  // }

  function handleDeadProcess(channel, peerUuid){    
    delete processes[peerUuid];
  }


  // This method is executed when a message from a connection is forwarded from another process
  // to this process, usually in response to a callback request that was sent from this process
  // to connections on other processes.
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

    if(messageObj !== undefined){
      communication.executeFunction({ connection: connections[connID] || {id: connID} }, messageObj);
    }
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


  // Adds a new "symbolic connection" to this process.
  //
  // channel here is a STRING in the form: "PRC:ProcessUUID:SYMNEW"
  // message here is a STRING of the form: {SymbolicData}

  function handleNewSymbolicConnection(channel, message){    

    var symbolicData = parseJSON(message);
    var symbolicConnID = symbolicData.nativeID;
    connections[symbolicConnID] = new SymbolicConnection(symbolicData);

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

    communication.executeFunction(executor, messageObj, createIPCCallBack);
  }


  // channel here is a STRING in the form: "PRC:processUUID:IPCCB"
  // message here is a STRING of the form: {owner: processUuid, func: functionName, ns: "interprocess", args: argsArray, callBack: callBackID}

  function handleIPCCallBack(channel, message){

    // debugProcessCallBacks("Handle IPC CallBack", channel, message);

    var messageObj = parseJSON(message);
    var executor = {connection: {id: messageObj.sender}}; // need to modify communication.callItBack to accept more than just connections as callback executors
    communication.executeFunction(executor, messageObj, createIPCCallBack);
  }

  function createIPCCallBack(id, sender, owner){

    // debugProcessCallBacks("Creating IPC Callback");

    var theCallBack = function(){
      var args = Array.prototype.slice.call(arguments);
      publish("PRC:"+owner+":IPCCB", JSON.stringify({ns: "internal", func: "callItBack", args: [id, args], sender: core.uuid}) );
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





  // symbolic connection handlers

  function handleSymbolicMessage(channel, message){

    // channel here is a STRING in the form: "SYM:symbolicConnID:MSG"
    // message here is a STRING of the form: "{samsaaraJSONFunctionCall}"

    var symbolicConnID = channel.split(":")[1];
    if(connections[symbolicConnID] !== undefined){
      communication.executeFunction({connection: connections[symbolicConnID] || {id: symbolicConnID} });
    }
  }

  function handleDeleteSymbolicConnection(channel, message){

    // channel here is a STRING in the form: "SYM:symbolicConnID:DEL"
    // message here is a STRING of the form: ""

    var symbolicConnID = channel.split(":")[1];

    if(connections[symbolicConnID] !== undefined){
      delete connections[symbolicConnID];
    }

    removeRoute("symMsg"+symbolicConnID);
    removeRoute("symDel"+symbolicConnID);
  }


  // native connection message handlers

  function handleMessageToNativeConnection(channel, message){

    // !! need to test this, individual subscriptions to native connections, vs. this single router?

    // channel here is a STRING in the form: "NTV:connID:MSG"
    // message here is a STRING of the form: "{samsaaraJSONFunctionCall}"

    var connID = channel.split(":")[1];

    if(connections[connID] !== undefined){
      connections[connID].write(message);
    }
  }




  // outgoing messages/objects

  function sendClientMessageToProcess(processID, connID, message){
    // debug("Publishing to", "PRC:"+processID+":FWD". message );
    publish("PRC:"+processID+":FWD", "FRM:"+connID+"::"+message);
  }

  function createSymbolicOnHost(connection, host, options){

    connection.symbolicOwners[host] = true;

    var symbolicData = {
      nativeID: connection.id,
      connectionData: connection.connectionData,
      owner: core.uuid
    };

    publish("PRC:"+host+":SYMNEW", JSON.stringify(symbolicData) );
  }


  // 
  // Connection Initialization Methods
  // Called for every new connection
  // 
  // @opts: {Object} contains the connection's options
  // @connection: {SamsaaraConnection} the connection that is initializing
  // @attributes: {Attributes} The attributes of the SamsaaraConnection and its methods
  // 

  function connectionInitialzation(opts, connection, attributes){

    debug("Initializing IPC Subscription!!!", core.uuid, opts.groups, connection.id);

    connection.symbolicOwners = {};    
    redisClient.incr("totalCurrentCount");        
    addRoute("ntvMsg"+connection.id, "NTV:"+connection.id+":MSG", handleMessageToNativeConnection);

    attributes.initialized(null, "ipc");
  }


  function connectionClosing(connection){
    redisClient.decr("totalCurrentCount");
    publish("SYM:" + connection.id + ":DEL", "");
    removeRoute("ntvMsg"+connection.id);
  }


  // 
  // Message Router
  // 

  function ipcRouteMessage(connection, owner, newHeader, message){        
    if(owner === core.uuid){
      router.processMessage(connection, message);
    }
    else{       
      sendClientMessageToProcess(owner, connection.id, message);
    }
  }

  function makeHeader(newHeader){
    var i = 1;
    if(newHeader === ""){
      newHeader += arguments[1];
      i++;
    }
    for(i; i < arguments.length; i++){
      newHeader += ":" + arguments[i];
    }
    return newHeader;
  }


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


  // Module Return Function.
  //
  // Within this function you should set up and return your samsaara middleWare exported
  // object. Your eported object can contain:
  // name, foundation, remoteMethods, connectionInitialization, connectionClose

  return function ipcRedis(samsaaraCore){

    core = samsaaraCore;

    connectionController = samsaaraCore.connectionController;
    communication = samsaaraCore.communication;
    router = samsaaraCore.router;
    connections = connectionController.connections;

    SymbolicConnection = require('./symbolic').initialize(samsaaraCore);
    Process = require('./process').initialize(samsaaraCore, processes, publish);

    // core.samsaara.createNamespace("interprocess", {
    //   testInterprocess: testInterprocess
    // });

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
      addRoute("addSymbolicConnection", "PRC:"+core.uuid+":SYMNEW", handleNewSymbolicConnection);

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


    var exported = {

      name: "ipc",

      moduleExports: {
        addRoute: addRoute,
        removeRoute: removeRoute,
        publishToRoute: publishToRoute,
        createSymbolicOnHost: createSymbolicOnHost,
        sendCallBackList: sendCallBackList,
        publish: publish,
        subscribe: subscribe,
        unsubscribe: unsubscribe,   
        subscribePattern: subscribePattern,
        unsubscribePattern: unsubscribePattern,
        routes: routes,
        store: redisClient,
        process: process
      },

      main: {
        createSymbolicOnHost: createSymbolicOnHost
      },

      connectionInitialization: {
        ipc: connectionInitialzation
      },

      connectionClose: {
        ipc: connectionClosing        
      },

      routeMessageOverride: ipcRouteMessage

    };

    return exported;

  };

}

module.exports = exports = ipcRedis;


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
