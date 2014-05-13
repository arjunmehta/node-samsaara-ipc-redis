/*!
 * Samsaara Inter Process Communication (Redis) Module
 * Copyright(c) 2014 Arjun Mehta <arjun@newlief.com>
 * MIT Licensed
 */

var redis = require('redis');

function ipcRedis(options){

var redisSub = redis.createClient(),
    redisPub = redis.createClient(),
    redisClient = redis.createClient();

  redisSub.on("message", function (channel, message) {
    switchMessages(channel, message);
  });

  var config,
      connectionController,
      communication,
      router,
      ipc;

  var routes = {};
  var routeList = {};


  /**
   * Main IPC Router Internal
   */

  function switchMessages(channel, message){
    // console.log(config.uuid, "NEW MESSAGE ON CHANNEL", channel, message);
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
    delete routes[routeList[routeName]];
    delete routeList[routeName];
  }

  function publishToRoute(routeName, message){
    publish(routes[routeList[routeName]], message);
  }

  function sendCallBackList(processID, callBackID, callBackList){

    // console.log("ipcRedis", config.uuid, "SENDING CALLBACK LIST ", processID, callBackID, callBackList);

    publish("PRC:"+processID+":CBL", callBackID+callBackList);
  }

  function sendCallBackExecute(process, callBackID, message){
    publish("PRC:"+processID+":CBX", callBackID+":"+callBackList);
  }




  /**
   * Base Pub/Sub
   */

  function publish(channel, message){
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

  function handleForwardedMessage(channel, message){

    console.log(config.uuid, "###Handling Forwarded Message", channel, message);

    var index = message.indexOf("::");
    var senderInfo = message.substring(0, index);
    var connMessage = message.slice(2+index-message.length);

    var senderInfoSplit = senderInfo.split(":");
    var connID = senderInfoSplit[senderInfoSplit.indexOf("FRM")+1];

    var messageObj = JSON.parse(connMessage);

    console.log("Process Message", senderInfoSplit, connID, JSON.parse(connMessage));

    communication.executeFunction({id: connID, owner: "IPC"}, messageObj);

  }

  function handleCallBackList(channel, message){    

    var callBackListSplit = message.split(":");
    var callBackID = callBackListSplit.shift();

    // console.log("ADDING CALL BACK CONNECTIONS", callBackID, callBackListSplit, communication.incomingCallBacks);
    
    communication.incomingCallBacks[callBackID].addConnections(callBackListSplit || []);
  }

  function sendClientMessageToProcess(processID, message){
    // console.log("Publishing to", "PRC:"+processID+":FWD". message );
    publish("PRC:"+processID+":FWD", message);
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
    console.log("Initializing IPC Subscription!!!", opts.groups, connection.id);
    redisSub.subscribe("NTV:"+connection.id+":MSG");
    redisClient.incr("totalCurrentCount");
    attributes.initialized(null, "ipc");
  }


  function connectionClosing(connection){
    var connID = connection.id;
    redisSub.unsubscribe("NTV"+connID+":MSG");
    redisClient.decr("totalCurrentCount");
  }


  /**
   * Message Router
   */

  function ipcRouteMessage(connection, owner, newPrepend, message){        
    if(owner === config.uuid){
      router.processMessage(connection, message);
    }
    else{
      newPrepend = "FRM:" + connection.id; // makePrepend("FRM", connection.id);
      communication.sendClientMessageToProcess(owner, newPrepend + "::" + message);
    }
  }

  function makePrepend(newPrepend){
    var i = 1;
    if(newPrepend === ""){
      newPrepend += arguments[1];
      i++;
    }
    for(i; i < arguments.length; i++){
      newPrepend += ":" + arguments[i];
    }
    return newPrepend;
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

    // console.log(samsaaraCore,);
    config = samsaaraCore.config;
    connectionController = samsaaraCore.connectionController;
    communication = samsaaraCore.communication;
    router = samsaaraCore.router;
    ipc = samsaaraCore.ipc;

    config.interProcess = true;

    communication.sendClientMessageToProcess = sendClientMessageToProcess;

    addRoute("process", "PRC:"+config.uuid+":FWD", handleForwardedMessage);
    addRoute("callBacks", "PRC:"+config.uuid+":CBL", handleCallBackList);

    var exported = {

      name: "ipcRedis",

      samsaaraCoreMethods: {
        addIPCRoute: addRoute,
        removeIPCRoute: removeRoute,
        publishToIPCRoute: publishToRoute,
        store: redisClient
      },

      foundationMethods: {
        addRoute: addRoute,
        removeRoute: removeRoute,
        publishToRoute: publishToRoute,
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


    // console.log("Returning exported", exported);
    return exported;

  };

}

module.exports = exports = ipcRedis;
