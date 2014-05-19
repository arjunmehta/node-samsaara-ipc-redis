/*!
 * Samsaara Inter Process Communication (Redis) Module
 * Copyright(c) 2014 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */

var debug = require('debug')('samsaara:ipcRedis');
var debugMessaging = require('debug')('samsaara:ipcRedis:messaging');

var redis = require('redis');

function ipcRedis(options){


  var config,
      connectionController,
      communication,
      router,
      ipc,
      connections;

  var symbolic = require('./symbolic');
  var SymbolicConnection = symbolic.SymbolicConnection;

  var routes = {},
      routeList = {};         

  var redisSub = redis.createClient(),
      redisPub = redis.createClient(),
      redisClient = redis.createClient();

  redisSub.on("message", function (channel, message) {
    switchMessages(channel, message);
  });



  /**
   * Main IPC Router Internal
   */

  function switchMessages(channel, message){
    debugMessaging("New Message On Channel", config.uuid, channel, message);
    routes[channel](channel, message);
  }


  /**
   * Foundation Methods
   */

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



  /**
   * Base Pub/Sub
   */

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




  /**
   * Router Methods (for IPC)
   */


  // incoming messages/objects

  /* 
   * This method is executed when a message from a connection is forwarded from another process
   * to this process, usually in response to a callback request that was sent from this process
   * to connections on other processes.
   */

  function handleForwardedMessage(channel, message){

    // channel here is a STRING in the form: "PRC:ProcessUUID:FWD"
    // message here is a STRING of the form: "FRM:connectionID::{samsaaraJSONFunctionCall}"

    debug("Handle Forwarded Message", config.uuid, channel, message);

    var index = message.indexOf("::");
    var senderInfo = message.substring(0, index);
    var connMessage = message.slice(2+index-message.length);

    var senderInfoSplit = senderInfo.split(":");
    var connID = senderInfoSplit[senderInfoSplit.indexOf("FRM")+1];

    var messageObj = parseJSON(connMessage);

    debug("Process Message", config.uuid, senderInfoSplit, connID, messageObj);

    if(messageObj !== undefined){
      communication.executeFunction({ connection: connections[connID] || {id: connID} }, messageObj);
    }
  }

  function handleCallBackList(channel, message){   

    // channel here is a STRING in the form: "PRC:ProcessUUID:CBL"
    // message here is a STRING of the form: "callBackID:connectionID:connectionID:..:connectionID"

    var callBackListSplit = message.split(":");
    var callBackID = callBackListSplit.shift();

    // debug("ADDING CALL BACK CONNECTIONS", callBackID, callBackListSplit, communication.incomingCallBacks);
    
    communication.incomingCallBacks[callBackID].addConnections(callBackListSplit || []);
  }

  function handleNewSymbolicConnection(channel, message){    

    // channel here is a STRING in the form: "PRC:ProcessUUID:SYMNEW"
    // message here is a STRING of the form: {SymbolicData}

    var symbolicData = parseJSON(message);
    var symbolicConnID = symbolicData.nativeID;

    connections[symbolicConnID] = new SymbolicConnection(symbolicData);    

    addRoute("symMsg"+symbolicConnID, "SYM:"+symbolicConnID+":MSG", handleSymbolicMessage);
    addRoute("symDel"+symbolicConnID, "SYM:"+symbolicConnID+":DEL", handleDeleteSymbolicConnection);

  }


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

  function handleMessageToNativeConnection(channel, message){

    // !! need to test this, individual subscriptions to native messages, vs a single router.

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
      owner: config.uuid
    };

    publish("PRC:"+host+":SYMNEW", JSON.stringify(symbolicData) );
  }

  function sendCallBackList(processID, callBackID, callBackList){
    // debug("ipcRedis", config.uuid, "SENDING CALLBACK LIST ", processID, callBackID, callBackList);
    publish("PRC:"+processID+":CBL", callBackID+callBackList);
  }

  function sendCallBackExecute(processID, callBackID, message){
    publish("PRC:"+processID+":CBX", callBackID+":"+callBackList);
  }



  /**
   * Connection Initialization Methods
   * Called for every new connection
   *
   * @opts: {Object} contains the connection's options
   * @connection: {SamsaaraConnection} the connection that is initializing
   * @attributes: {Attributes} The attributes of the SamsaaraConnection and its methods
   */

  function connectionInitialzation(opts, connection, attributes){

    debug("Initializing IPC Subscription!!!", config.uuid, opts.groups, connection.id);

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


  /**
   * Message Router
   */

  function ipcRouteMessage(connection, owner, newHeader, message){        
    if(owner === config.uuid){
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


  /**
   * Message Filters
   */



  /**
   * Module Return Function.
   * Within this function you should set up and return your samsaara middleWare exported
   * object. Your eported object can contain:
   * name, foundation, remoteMethods, connectionInitialization, connectionClose
   */

  return function ipcRedis(samsaaraCore){

    // debug(samsaaraCore,);
    config = samsaaraCore.config;
    connectionController = samsaaraCore.connectionController;
    communication = samsaaraCore.communication;
    router = samsaaraCore.router;
    ipc = samsaaraCore.ipc;
    connections = connectionController.connections;

    symbolic.initialize(samsaaraCore);

    config.interProcess = true;

    // handles messages that are redirected to this process
    addRoute("process", "PRC:"+config.uuid+":FWD", handleForwardedMessage);

    // handles a list of connections that we are expecting a callback from
    addRoute("callBackList", "PRC:"+config.uuid+":CBL", handleCallBackList);

    // creates a new symbolic connection
    addRoute("addSymbolicConnection", "PRC:"+config.uuid+":SYMNEW", handleNewSymbolicConnection);

    // creates a new symbolic connection
    addRoute("deleteSymbolicConnection", "PRC:"+config.uuid+":SYMDEL", handleDeleteSymbolicConnection);

    var exported = {

      name: "ipc",

      samsaaraCoreMethods: {
        addIPCRoute: addRoute,
        removeIPCRoute: removeRoute,
        publishToIPCRoute: publishToRoute,
        createSymbolicOnHost: createSymbolicOnHost,
        store: redisClient
      },

      foundationMethods: {
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
        ipcRoutes: routes,
        store: redisClient
      },

      remoteMethods: {
      },

      connectionInitialization: {
        ipc: connectionInitialzation
      },

      connectionClose: {
        ipc: connectionClosing        
      },

      messageFilters: {
      },

      routeMessageOverride: ipcRouteMessage

    };


    // debug("Returning exported", exported);
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
