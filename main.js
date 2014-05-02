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
    console.log(config.uuid, "NEW MESSAGE ON CHANNEL", channel, message)
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

  function sendCallBackList(process, callBackID, callBackList){
    publish("PRC:"+processID+":CB", callBackID+":"+callBackList);
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
    var index = message.indexOf("::");
    var senderInfo = message.substr(0, index);
    var connMessage = message.slice(2+index-message.length);

    var senderInfoSplit = senderInfo.split(":");
    var connID = senderInfoSplit[0];
    var connToken = senderInfoSplit[1];

    connectionController.connections[connID].preprocessMessage(message);
  }

  function handleCallBackList(channel, message){
    var callBackListSplit = message.split(":");
    var connectionsArray =  message.split(":");
    connectionsArray.pop();
    // console.log("ADDING CALL BACK CONNECTIONS", callBackID, connectionsArray);
    communication.incomingCallBacks[callBackID].addConnections(connectionsArray);
  }

  function sendClientMessageToProcess(processID, message){
    console.log("Publishing to", "PRC:"+processID+":FWD" );
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
    attributes.initialized(null, "ipc");
  }


  function connectionClosing(connection){
    var connID = connection.id;
    redisSub.unsubscribe("NTV"+connID+":MSG");
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

    communication.sendClientMessageToProcess = sendClientMessageToProcess;

    addRoute("process", "PRC:"+config.uuid+":FWD", handleProcessMessage);
    addRoute("callBacks", "PRC:"+config.uuid+":CB", handleCallBackList);

    samsaaraCore.Connection.prototype.routeMessage = function(message){
      var route = message.substr(2,8);
      // console.log(config.uuid, "MESSAGE ROUTE", route);
      if(route === config.uuid){
        this.preprocessMessage(message);
      }
      else{
        console.log("TRYING TO ROUTE MESSAGE TO", "PRC:"+route+":RCV:"+this.id);
        communication.sendClientMessageToProcess(route, this.id+":"+this.token+"::"+message);
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
