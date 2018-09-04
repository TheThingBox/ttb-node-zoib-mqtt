/*
    Copyright (c) 2015 Digital Airways (www.DigitalAirways.com)
    This work is free. You can redistribute it and/or modify it under the
    terms of the "Do What The Fuck You Want To" Public License, Version 2,
    as published by Sam Hocevar.
    See http://www.wtfpl.net for more details.
*/

module.exports = function(RED) {
    "use strict";
    var mqtt = require("mqtt");
    var isUtf8 = require("is-utf8");
    var request = require("request");
    var crypto = require("crypto");

    var currentTopic = {};
    var currentQos = {};
    var currentBroker = {};

    var brokers = {
        list: [],
        get: function(n) {
            var b = {};
            var index = -1;
            b.broker = n.broker;
            b.port = n.port;
            b.clientid = n.clientid;
            b.usetls = n.usetls;
            b.verifyservercert = n.verifyservercert;
            b.compatmode = n.compatmode;
            b.keepalive = n.keepalive;
            b.cleansession = n.cleansession;
            b.credentials = n.credentials;

            // If the config node is missing certain options (it was probably deployed prior to an update to the node code),
            // select/generate sensible options for the new fields
            if (typeof b.usetls === 'undefined') {
                b.usetls = false;
            }
            if (typeof b.compatmode === 'undefined') {
                b.compatmode = true;
            }
            if (typeof b.verifyservercert === 'undefined') {
                b.verifyservercert = false;
            }
            if (typeof b.keepalive === 'undefined') {
                b.keepalive = 60;
            } else if (typeof b.keepalive === 'string') {
                b.keepalive = Number(b.keepalive);
            }
            if (typeof b.cleansession === 'undefined' || (!b.cleansession && !b.clientid)) {
                b.cleansession = true;
            }
            if (b.broker == "127.0.0.1") {
                b.broker = "localhost";
            }

            b.brokerkey = b.broker + b.port + b.clientid + b.usetls + b.verifyservercert + b.compatmode + b.keepalive + b.cleansession + JSON.stringify(b.credentials)

            for (var i in brokers.list) {
                if (brokers.list[i].brokerkey == b.brokerkey) {
                    index = i;
                    break;
                }
            }

            if (index == -1) {
                index = brokers.list.length;
                // Config node state
                b.brokerurl = "";
                b.connected = false;
                b.connecting = false;
                b.closing = false;
                b.options = {};
                b.queue = [];
                b.subscriptions = {};

                if (n.birthTopic) {
                    b.birthMessage = {
                        topic: n.birthTopic,
                        payload: n.birthPayload || "",
                        qos: Number(n.birthQos || 0),
                        retain: n.birthRetain == "true" || n.birthRetain === true
                    };
                }

                // Create the URL to pass in to the MQTT.js library
                if (b.brokerurl === "") {
                    if (b.usetls) {
                        b.brokerurl = "mqtts://";
                    } else {
                        b.brokerurl = "mqtt://";
                    }
                    if (b.broker !== "") {
                        b.brokerurl = b.brokerurl + b.broker + ":" + b.port;
                    } else {
                        b.brokerurl = b.brokerurl + "localhost:1883";
                    }
                }

                // Build options for passing to the MQTT.js API
                b.options.clientId = b.clientid || 'mqtt_' + (1 + Math.random() * 4294967295).toString(16);
                b.options.keepalive = b.keepalive;
                b.options.clean = b.cleansession;
                b.options.reconnectPeriod = RED.settings.mqttReconnectTime || 5000;
                if (b.compatmode == "true" || b.compatmode === true) {
                    b.options.protocolId = 'MQIsdp';
                    b.options.protocolVersion = 3;
                }
                if (b.usetls && n.tls) {
                    var tlsNode = RED.nodes.getNode(n.tls);
                    if (tlsNode) {
                        tlsNode.addTLSOptions(b.options);
                    }
                }
                // If there's no rejectUnauthorized already, then this could be an
                // old config where this option was provided on the broker node and
                // not the tls node
                if (typeof b.options.rejectUnauthorized === 'undefined') {
                    b.options.rejectUnauthorized = (b.verifyservercert == "true" || b.verifyservercert === true);
                }

                if (n.willTopic) {
                    b.options.will = {
                        topic: n.willTopic,
                        payload: n.willPayload || "",
                        qos: Number(n.willQos || 0),
                        retain: n.willRetain == "true" || n.willRetain === true
                    };
                }

                b.users = {};
                b.reconnectTimeout = null

                b.reconnect = function(){
                    console.log("--------------------------------------------------")
                    console.log("mqtt.reconnect")
                    if (!b.connected && !b.connecting) {
                      if(b.reconnectTimeout){
                        clearTimeout(b.reconnectTimeout)
                      }
                      b.reconnectTimeout = setTimeout(() => {
                        if (!b.connected && !b.connecting) {
                          b.connect()
                        } else {
                          b.reconnect()
                        }
                      }, b.options.reconnectPeriod)
                    }
                }

                b.register = function(mqttNode) {
                    b.users[mqttNode.id] = mqttNode;
                    if (Object.keys(b.users).length === 1) {
                        b.connect();
                    }
                };

                b.deregister = function(mqttNode, done) {
                    if (!b.users[mqttNode.id]) {
                        return done();
                    }
                    delete b.users[mqttNode.id];
                    if (b.closing) {
                        return done();
                    }
                    if (Object.keys(b.users).length === 0) {
                        if (b.client && b.client.connected) {
                            return b.client.end(done);
                        } else {
                            b.client.end();
                            return done();
                        }
                    }
                    done();
                };

                b.connect = function() {
                    if (!b.connected && !b.connecting) {
                        b.connecting = true;

                        let zoib_user = ""
                        let zoib_password = ""
                        let zoib_server = ""
                        if(b.credentials) {
                          zoib_user = b.credentials.username
                          zoib_password = b.credentials.password
                          zoib_server = b.credentials.zoib
                        }
                        getZoibToken(zoib_user, zoib_password, zoib_server, (err, token) => {
                            if(token){
                                b.options.username = 'zosquitto';
                                b.options.password = token;
                            }

                            console.log("--------------------------------------------------")
                            console.log("mqtt.connect", 'token :', token)
                            b.client = mqtt.connect(b.brokerurl, b.options);
                            b.client.setMaxListeners(0);
                            // Register successful connect or reconnect handler
                            b.client.on('connect', function() {
                                console.log("--------------------------------------------------")
                                console.log("mqtt.on.connect")
                                b.connecting = false;
                                b.connected = true;
                                for (var id in b.users) {
                                    if (b.users.hasOwnProperty(id)) {
                                        b.users[id].connected();
                                    }
                                }
                                // Remove any existing listeners before resubscribing to avoid duplicates in the event of a re-connection
                                b.client.removeAllListeners('message');

                                // Re-subscribe to stored topics
                                for (var s in b.subscriptions) {
                                    if (b.subscriptions.hasOwnProperty(s)) {
                                        var topic = s;
                                        var qos = 0;
                                        for (var r in b.subscriptions[s]) {
                                            if (b.subscriptions[s].hasOwnProperty(r)) {
                                                qos = Math.max(qos, b.subscriptions[s][r].qos);
                                                b.client.on('message', b.subscriptions[s][r].handler);
                                            }
                                        }
                                        var options = { qos: qos };
                                        b.client.subscribe(topic, options);
                                    }
                                }

                                // Send any birth message
                                if (b.birthMessage) {
                                    b.publish(b.birthMessage);
                                }
                            });

                            b.client.on("reconnect", function() {
                                console.log("--------------------------------------------------")
                                console.log("mqtt.on.reconnect")
                                for (var id in b.users) {
                                    if (b.users.hasOwnProperty(id)) {
                                        b.users[id].connecting();
                                    }
                                }
                            })
                            // Register disconnect handlers

                            b.client.on('close', function() {
                                console.log("--------------------------------------------------")
                                console.log("mqtt.on.close")
                                if (b.connected) {
                                    b.connected = false;
                                    for (var id in b.users) {
                                        if (b.users.hasOwnProperty(id)) {
                                            b.users[id].disconnected();
                                        }
                                    }
                                    b.client.end();
                                    b.reconnect();
                                }
                            });

                            // Register connect error handler
                            b.client.on('error', function(error) {
                                console.log("--------------------------------------------------")
                                console.log("mqtt.on.error")
                                if (b.connecting) {
                                    b.client.end();
                                    b.connecting = false;
                                }
                            });
                        });
                    }
                };

                b.subscribe = function(topic, qos, callback, ref) {
                    ref = ref || 0;
                    b.subscriptions[topic] = b.subscriptions[topic] || {};
                    var sub = {
                        topic: topic,
                        qos: qos,
                        handler: function(mtopic, mpayload, mpacket) {
                            if (matchTopic(topic, mtopic)) {
                                callback(mtopic, mpayload, mpacket);
                            }
                        },
                        ref: ref
                    };
                    b.subscriptions[topic][ref] = sub;
                    if (b.connected) {
                        b.client.on('message', sub.handler);
                        var options = {};
                        options.qos = qos;
                        b.client.subscribe(topic, options);
                    }
                };

                b.unsubscribe = function(topic, ref) {
                    ref = ref || 0;
                    var sub = b.subscriptions[topic];
                    if (sub) {
                        if (sub[ref]) {
                            b.client.removeListener('message', sub[ref].handler);
                            delete sub[ref];
                        }
                        if (Object.keys(sub).length === 0) {
                            delete b.subscriptions[topic];
                            if (b.connected) {
                                b.client.unsubscribe(topic);
                            }
                        }
                    }
                };

                b.publish = function(msg) {
                    if (b.connected) {
                        if (!Buffer.isBuffer(msg.payload)) {
                            if (typeof msg.payload === "object") {
                                msg.payload = JSON.stringify(msg.payload);
                            } else if (typeof msg.payload !== "string") {
                                msg.payload = "" + msg.payload;
                            }
                        }

                        var options = {
                            qos: msg.qos || 0,
                            retain: msg.retain || false
                        };
                        b.client.publish(msg.topic, msg.payload, options, function(err) {
                            return
                        });
                    }
                };

                brokers.list[index] = b;
            }
            return brokers.list[index];
        }
    }

    function getZoibToken(user, password, server, callback) {
      if(!user || !password || !server){
        return callback('missing params')
      }
      request({
        method: 'GET',
        url: `${server}/challenge`,
        qs: { login: user}
      }, function(err, response, body) {
        try{
          body = JSON.parse(body)
        } catch(e) {}
        if(err || !body || !body.challenge) {
          callback(err || 'error on get challenge')
          return
        }
        let pwd = crypto.createHash('sha512').update(password).digest('hex').toUpperCase()
        let challenge = crypto.createHash('sha512').update(`${body.challenge}-${user}-${pwd}`).digest('hex').toUpperCase()
        request({
          method: 'POST',
          url: `${server}/login`,
          json: true,
          body: { login: user, challenge: challenge }
        }, function(err2, response2, body2) {
            try{
              body2 = JSON.parse(body2)
            } catch(e) {}
            if(err2 || !body2 || !body2.token) {
              callback(err2 || 'error on post login')
              return
            }
            callback(null, body2.token)
        })
      });
    }

    function matchTopic(ts, t) {
        if (ts == "#") {
            return true;
        }
        var re = new RegExp("^" + ts.replace(/([\[\]\?\(\)\\\\$\^\*\.|])/g, "\\$1").replace(/\+/g, "[^/]+").replace(/\/#$/, "(\/.*)?") + "$");
        return re.test(t);
    }

    function canHaveEarlyBroker(conf) {
        if (conf.broker && (conf.brokerType == "str" || conf.brokerType == "defBroker") &&
            conf.port && (conf.portType == "num" || conf.portType == "defPort") &&
            (conf.clientidType == "auto" || conf.clientidType == "str") &&
            conf.keepalive && conf.keepaliveType == "num" &&
            conf.cleansession && conf.cleansessionType == "bool" &&
            conf.compatmode && conf.compatmodeType == "bool" &&
            conf.user && conf.userType == "str"  &&
            conf.password && conf.passwordType == "str"  &&
            conf.zoib && conf.zoibType == "str"
        ) {
            return true;
        }

        return false;
    }

    function compareBrokerConfig(a, b) {
        if (a && b &&
            typeof a === "object" &&
            typeof b === "object" &&
            (a.broker == b.broker || ((a.broker == "127.0.0.1" || a.broker == "localhost") && (b.broker == "127.0.0.1" || b.broker == "localhost"))) &&
            a.port == b.port &&
            a.clientid == b.clientid &&
            a.keepalive == b.keepalive &&
            a.cleansession == b.cleansession &&
            a.compatmode == b.compatmode &&
            ((!a.credentials && !b.credentials) || (a.credentials && b.credentials && a.credentials.user == b.credentials.user && a.credentials.password == b.credentials.password && a.credentials.zoib == b.credentials.zoib))
        ) {
            return true;
        }

        return false;
    }

    function getProperty(param, msg, prop, propType, set, type, def, canBeEmpty, warn) {
        var p = null;

        if (param[propType] == "msg") {
            try {
                p = RED.util.getMessageProperty(msg, param[prop]);
            } catch (err) {}
            if (typeof p === "string" && p.trim() == "" && canBeEmpty) {
                p = "";
            } else if (!p && warn) {
                warn("msg." + param[prop] + " is undefined");
            }
        } else if (set && param[propType] == set.type) {
            try {
                p = RED.util.getMessageProperty(msg, set.value);
            } catch (err) {}
            if (typeof p === "string" && p.trim() == "" && canBeEmpty) {
                p = "";
            } else if (!p && warn) {
                warn("msg." + set.value + " is undefined");
            }
        } else if (type && param[propType] == type) {
            p = param[prop];
        } else if (def && param[propType] == def) {
            p = param[prop];
        }
        return p;
    }

    function mainInput(config) {
        RED.nodes.createNode(this, config);

        this.param = {
            broker: config.broker,
            brokerType: config.brokerType,
            port: config.port,
            portType: config.portType,
            clientid: config.clientid || "",
            clientidType: config.clientidType,
            keepalive: config.keepalive,
            keepaliveType: config.keepaliveType,
            cleansession: config.cleansession,
            cleansessionType: config.cleansessionType,
            compatmode: config.compatmode,
            compatmodeType: config.compatmodeType,
            topic: config.topic,
            topicType: config.topicType,
            qos: config.qos,
            qosType: config.qosType,
            user: config.user,
            userType: config.userType,
            password: config.password,
            passwordType: config.passwordType,
            zoib: config.zoib,
            zoibType: config.zoibType
        };

        if (this.param.qosType == "numQos") {
            this.param.qos = parseInt(this.param.qos);
            if (isNaN(this.param.qos) || this.param.qos < 0 || this.param.qos > 2) {
                this.param.qos = 2;
            }
        }

        if (this.param.portType == "num") {
            this.param.port = parseInt(this.param.port);
            if (isNaN(this.param.port) || this.param.port < 0) {
                this.param.port = 1883;
            }
        } else if (this.param.portType == "defPort") {
            this.param.port = 1883;
        }

        if (this.param.keepaliveType == "num") {
            this.param.keepalive = parseInt(this.param.keepalive);
            if (isNaN(this.param.keepalive) || this.param.keepalive < 0) {
                this.param.keepalive = 15;
            }
        }

        if (this.param.brokerType == "defBroker") {
            this.param.broker = "localhost";
        }

        if (this.param.clientidType == "auto") {
            this.param.clientid = "";
        }

        if (this.param.userType == "none") {
            this.param.user = "";
        }

        if (this.param.passwordType == "none") {
            this.param.password = "";
        }

        this.brokerConn = null;
        var node = this;


        node.connect = function(broker) {
            if (currentBroker[node.id] && !compareBrokerConfig(broker, currentBroker[node.id])) {
                if (currentTopic[node.id]) {
                    brokers.get(currentBroker[node.id]).unsubscribe(currentTopic[node.id], node.id);
                    node.topic = null;
                }
                brokers.get(currentBroker[node.id]).deregister(node, function() {});
            } else if (currentBroker[node.id] && node.param.brokerConn) {
                return true;
            }

            currentBroker[node.id] = broker;
            node.param.brokerConn = brokers.get(currentBroker[node.id]);

            if (node.param.brokerConn) {
                node.param.brokerConn.register(node);
                return true;
            }

            node.warn("bad config", broker);
            node.status({ fill: "red", shape: "ring", text: currentBroker[node.id].broker + ":" + currentBroker[node.id].port });
            return false;
        }

        node.subscribe = function(topic) {
            if (!/^(#$|(\+|[^+#]*)(\/(\+|[^+#]*))*(\/(\+|#|[^+#]*))?$)/.test(topic)) {
                node.warn("wrong topic :" + topic.topic)
            } else if (!topic.topic && node.topic) {
                node.param.brokerConn.unsubscribe(node.topic, node.id);
                node.status({ fill: "green", shape: "dot", text: currentBroker[node.id].broker + ":" + currentBroker[node.id].port });
                currentTopic[node.id] = null;
                currentQos[node.id] = null;
            } else if (topic.topic) {
                if (node.topic) {
                    node.param.brokerConn.unsubscribe(node.topic, node.id);
                }
                node.topic = topic.topic;
                node.qos = topic.qos;
                currentTopic[node.id] = topic.topic;
                currentQos[node.id] = topic.qos;
                node.param.brokerConn.subscribe(node.topic, node.qos, function(topic, payload, packet) {
                    if (!node.param.brokerConn) {
                        return;
                    }
                    if (isUtf8(payload)) { payload = payload.toString(); }
                    var msg = { topic: topic, payload: payload, qos: packet.qos, retain: packet.retain };
                    if ((node.param.brokerConn.broker === "localhost") || (node.param.brokerConn.broker === "127.0.0.1")) {
                        msg._topic = topic;
                    }
                    node.send(msg);
                }, node.id);
                if (node.param.brokerConn.connected) {
                    node.status({ fill: "green", shape: "dot", text: "sub on " + currentBroker[node.id].broker + ":" + currentBroker[node.id].port + " @ " + node.topic });
                }
            }
        }

        if (canHaveEarlyBroker(node.param)) {
            if (node.connect({
                    broker: node.param.broker,
                    port: node.param.port,
                    clientid: node.param.clientid,
                    compatmode: node.param.compatmode,
                    keepalive: node.param.keepalive,
                    cleansession: node.param.cleansession,
                    credentials: {
                        username: node.param.user,
                        password: node.param.password,
                        zoib: node.param.zoib
                    }
                }))
            {
                if (node.param.topicType == "str" && node.param.qosType == "numQos") {
                    node.subscribe({
                        topic: node.param.topic,
                        qos: node.param.qos
                    });
                } else if (currentTopic[node.id] && currentQos[node.id]) {
                    node.subscribe({
                        topic: currentTopic[node.id],
                        qos: currentQos[node.id]
                    });
                }
            }
        }

        this.connected = function() {
            var text = currentBroker[node.id].broker + ":" + currentBroker[node.id].port;
            if (currentTopic[node.id]) {
                text = "sub on " + text + " @ " + currentTopic[node.id];
            }
            node.status({ fill: "green", shape: "dot", text: text });
        }

        this.connecting = function() {
            node.status({ fill: "red", shape: "ring", text: "connecting to " + currentBroker[node.id].broker + ":" + currentBroker[node.id].port });
        }

        this.disconnected = function() {
            node.status({ fill: "red", shape: "ring", text: currentBroker[node.id].broker + ":" + currentBroker[node.id].port });
        }

        this.on('input', function(msg) {
            var credentials = undefined;
            let zoib_user = getProperty(node.param, msg, "user", "userType", { value: "user", type: "setUser" }, "str", "none", true, node.warn)
            let zoib_password = getProperty(node.param, msg, "password", "passwordType", { value: "password", type: "setPassword" }, "str", "none", true, node.warn)
            let zoib_server = getProperty(node.param, msg, "zoib", "zoibType", { value: "zoib.server", type: "setZoib" }, "str", "none", true, node.warn)
            if(zoib_user && zoib_password && zoib_server){
              credentials = {
                  username: zoib_user,
                  password: zoib_password,
                  zoib: zoib_server
              }
            }
            if (node.connect({
                    broker: getProperty(node.param, msg, "broker", "brokerType", { value: "broker.server", type: "setBroker" }, "str", "defBroker", false, node.warn),
                    port: getProperty(node.param, msg, "port", "portType", { value: "broker.port", type: "setPort" }, "num", "defPort", false, node.warn),
                    clientid: getProperty(node.param, msg, "clientid", "clientidType", { value: "broker.clientid", type: "setClientid" }, "str", "auto", true, node.warn),
                    compatmode: getProperty(node.param, msg, "compatmode", "compatmodeType", { value: "broker.compatmode", type: "setCompatmode" }, "bool", false, false, node.warn),
                    keepalive: getProperty(node.param, msg, "keepalive", "keepaliveType", { value: "broker.keepalive", type: "setKeepalive" }, "num", false, false, node.warn),
                    cleansession: getProperty(node.param, msg, "cleansession", "cleansessionType", { value: "broker.cleansession", type: "setCleansession" }, "bool", false, false, node.warn),
                    credentials: credentials,
                })) {
                node.subscribe({
                    topic: getProperty(node.param, msg, "topic", "topicType", { value: "topic", type: "setTopic" }, "str", false, true, node.warn),
                    qos: getProperty(node.param, msg, "qos", "qosType", { value: "qos", type: "setQos" }, false, "numQos", false, node.warn)
                });
            }
        });
        this.on('close', function(done) {
            if (node.param.brokerConn) {
                if (currentTopic[node.id]) {
                    node.param.brokerConn.unsubscribe(currentTopic[node.id], node.id);
                }
                node.param.brokerConn.deregister(node, done);
                node.param.brokerConn = null;
            } else {
                done()
            }
        });
    }
    RED.nodes.registerType("zoib-input-mqtt", mainInput);

    function mainOutput(config) {
        RED.nodes.createNode(this, config);

        this.param = {
            broker: config.broker,
            brokerType: config.brokerType,
            port: config.port,
            portType: config.portType,
            clientid: config.clientid || "",
            clientidType: config.clientidType,
            keepalive: config.keepalive,
            keepaliveType: config.keepaliveType,
            cleansession: config.cleansession,
            cleansessionType: config.cleansessionType,
            compatmode: config.compatmode,
            compatmodeType: config.compatmodeType,
            topic: config.topic,
            topicType: config.topicType,
            qos: config.qos,
            qosType: config.qosType,
            retain: config.retain,
            retainType: config.retainType,
            user: config.user,
            userType: config.userType,
            password: config.password,
            passwordType: config.passwordType,
            zoib: config.zoib,
            zoibType: config.zoibType
        };

        if (this.param.qosType == "numQos") {
            this.param.qos = parseInt(this.param.qos);
            if (isNaN(this.param.qos) || this.param.qos < 0 || this.param.qos > 2) {
                this.param.qos = 2;
            }
        }

        if (this.param.portType == "num") {
            this.param.port = parseInt(this.param.port);
            if (isNaN(this.param.port) || this.param.port < 0) {
                this.param.port = 1883;
            }
        } else if (this.param.portType == "defPort") {
            this.param.port = 1883;
        }

        if (this.param.keepaliveType == "num") {
            this.param.keepalive = parseInt(this.param.keepalive);
            if (isNaN(this.param.keepalive) || this.param.keepalive < 0) {
                this.param.keepalive = 15;
            }
        }

        if (this.param.brokerType == "defBroker") {
            this.param.broker = "localhost";
        }

        if (this.param.clientidType == "auto") {
            this.param.clientid = "";
        }

        if (this.param.userType == "none") {
            this.param.user = "";
        }

        if (this.param.passwordType == "none") {
            this.param.password = "";
        }

        this.brokerConn = null;
        var node = this;

        node.connect = function(broker) {
            if (currentBroker[node.id] && !compareBrokerConfig(broker, currentBroker[node.id])) {
                brokers.get(currentBroker[node.id]).deregister(node, function() {});
            } else if (currentBroker[node.id] && node.param.brokerConn) {
                return true;
            }

            currentBroker[node.id] = broker;
            node.param.brokerConn = brokers.get(currentBroker[node.id]);

            if (node.param.brokerConn) {
                node.param.brokerConn.register(node);
                return true;
            }

            node.warn("bad config", broker);
            node.status({ fill: "red", shape: "ring", text: currentBroker[node.id].broker + ":" + currentBroker[node.id].port });
            return false;
        }

        this.connected = function() {}

        this.connecting = function() {}

        this.disconnected = function() {}

        this.on('input', function(msg) {
            var broker = getProperty(node.param, msg, "broker", "brokerType", { value: "broker.server", type: "setBroker" }, "str", "defBroker", false, node.warn);
            var port = getProperty(node.param, msg, "port", "portType", { value: "broker.port", type: "setPort" }, "num", "defPort", false, node.warn);
            var credentials = undefined;
            let zoib_user = getProperty(node.param, msg, "user", "userType", { value: "user", type: "setUser" }, "str", "none", true, node.warn)
            let zoib_password = getProperty(node.param, msg, "password", "passwordType", { value: "password", type: "setPassword" }, "str", "none", true, node.warn)
            let zoib_server = getProperty(node.param, msg, "zoib", "zoibType", { value: "zoib.server", type: "setZoib" }, "str", "none", true, node.warn)

            if(zoib_user && zoib_password && zoib_server){
              credentials = {
                  username: zoib_user,
                  password: zoib_password,
                  zoib: zoib_server
              }
            }
            if (node.connect({
                    broker: broker,
                    port: port,
                    clientid: getProperty(node.param, msg, "clientid", "clientidType", { value: "broker.clientid", type: "setClientid" }, "str", "auto", true, node.warn),
                    compatmode: getProperty(node.param, msg, "compatmode", "compatmodeType", { value: "broker.compatmode", type: "setCompatmode" }, "bool", false, false, node.warn),
                    keepalive: getProperty(node.param, msg, "keepalive", "keepaliveType", { value: "broker.keepalive", type: "setKeepalive" }, "num", false, false, node.warn),
                    cleansession: getProperty(node.param, msg, "cleansession", "cleansessionType", { value: "broker.cleansession", type: "setCleansession" }, "bool", false, false, node.warn),
                    credentials: credentials
                })) {
                var retain = getProperty(node.param, msg, "retain", "retainType", { value: "retain", type: "setRetain" }, "bool", false, false, node.warn);
                retain = ((retain === true) || (retain === "true")) || false;
                var topic = getProperty(node.param, msg, "topic", "topicType", { value: "topic", type: "setTopic" }, "str", false, true, node.warn);
                if (/^([^\+#\/]*$|([^\+#]+\/[^\+#]*)*[^\+#]$)/g.test(topic)) {
                    node.status({ fill: "green", shape: "dot", text: "pub on " + broker + ":" + port + " @ " + topic });
                    node.param.brokerConn.publish({
                        payload: msg.payload,
                        topic: topic,
                        qos: Number(getProperty(node.param, msg, "qos", "qosType", { value: "qos", type: "setQos" }, false, "numQos", false, node.warn)),
                        retain: retain
                    });
                } else {
                    node.status({ fill: "red", shape: "ring", text: "wrong topic : " + topic });
                }
            }
        });
        this.on('close', function(done) {
            if (node.param.brokerConn) {
                node.param.brokerConn.deregister(node, done);
                node.param.brokerConn = null;
            } else {
                done()
            }
        });
    }
    RED.nodes.registerType("zoib-output-mqtt", mainOutput);
}
