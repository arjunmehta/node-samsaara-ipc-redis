/*!
 * Samsaara Inter Process Communication (Redis) Module
 * Copyright(c) 2013 Arjun Mehta <arjun@newlief.com>
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
      ipc;

  var routes = {};
  var routeList = {};




  /**
   * Main IPC Router
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

  function handleProcessMessage(channel, message){

    console.log(config.uuid, "###Handling process Message", channel, message);

    var index = message.indexOf("::");
    var senderInfo = message.substring(0, index);
    var connMessage = message.slice(2+index-message.length);

    var senderInfoSplit = senderInfo.split(":");
    var connID = senderInfoSplit[0];
    var connToken = senderInfoSplit[1];

    var messageObj = JSON.parse(connMessage)[1];

    console.log("Process Message", senderInfoSplit, connID, JSON.parse(connMessage)[1]);

    messageObj.sender = connID;

    if(messageObj.func !== undefined){
      communication.executeFunction({id: connID}, messageObj);
    }
    else if(messageObj.internal !== undefined){
      messageObj.ns = "internal";
      messageObj.func = messageObj.internal;
      communication.executeFunction({id: connID}, messageObj);
    }

    // connectionController.Connection.prototype.receiveMessage.call(, JSON.parse(connMessage)[1]);

    // connectionController.connections[connID].preprocessMessage(message);
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
    ipc = samsaaraCore.ipc;

    config.interProcess = true;

    communication.sendClientMessageToProcess = sendClientMessageToProcess;

    addRoute("process", "PRC:"+config.uuid+":FWD", handleProcessMessage);
    addRoute("callBacks", "PRC:"+config.uuid+":CBL", handleCallBackList);

    samsaaraCore.connectionController.Connection.prototype.routeMessage = function(message){
      var route = message.substr(2,8);
      // console.log(config.uuid, "MESSAGE ROUTE", route);
      if(route === config.uuid){
        this.preprocessMessage(message);
      }
      else{
        // console.log("TRYING TO ROUTE MESSAGE TO", "PRC:"+route+":FWD:"+this.id);
        communication.sendClientMessageToProcess(route, this.id+":"+this.token+"::"+message);
        //this.process.token
        //interprocess communication + authorization + modular!!!
      }          
    };

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
        routes: routes,
        store: redisClient
      },

      remoteMethods: {
      },

      connectionInitialization: {
        ipc: connectionInitialzation
      },

      connectionClose: {
        ipc: connectionClosing        
      }
    };


    // console.log("Returning exported", exported);
    return exported;

  };

}

module.exports = exports = ipcRedis;
