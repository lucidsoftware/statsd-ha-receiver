var dgram  = require('dgram'),
    util   = require('util'),
    net    = require('net'),
    config = require('./lib/config'),
    fs     = require('fs'),
    events = require('events'),
    logger = require('./lib/logger');

var startupTime = Math.round(new Date().getTime() / 1000);
var conf;
var l;

var stats = {
	messages: {
		last_msg_seen: startupTime,
		bad_lines_seen: 0
	}
};

var configDefaults = {
	port: 8125,
	address: '0.0.0.0',
	protocol: 'tcp4',
	flushInterval: 10000,
	debug: false,
	dumpMessages: false,
	prefix: '',
	replication: 1,
	batchSize: 1024,
	aggregation: []
};

function normalizeConfig(config) {
	var key, i;

	for (key in configDefaults) {
		if (configDefaults.hasOwnProperty(key) && !config.hasOwnProperty(key)) {
			config[key] = configDefaults[key];
		}
	}
}

function validateConfig(config) {
	if (!config.hasOwnProperty('shardRingSize')) {
		throw "shardRingSize must be configured!";
	}
	if (!config.hasOwnProperty('shards')) {
		throw "shards must be configured!";
	}

	for (i = 0; i < config.shards.length; i++) {
		var shard = config.shards[i];
		if (!shard.hasOwnProperty('hostname')) {
			throw "shard hostname must be configured!";
		}
		if (!shard.hasOwnProperty('port')) {
			throw "shard port must be configured!";
		}
		if (!shard.hasOwnProperty('protocol')) {
			throw "shard protocol must be configured!";
		}
		if (!shard.hasOwnProperty('ringIndex')) {
			throw "shard ringIndex must be configured!";
		}
	}
}

function processLine(line, fromSocket) {
	if (conf.dumpMessages && fromSocket) {
		l.log("received: " + line);
	}
}

function processLines(lines, fromSocket) {
	for(var i = 0; i < lines.length; i++) {
		processLine(lines[i], fromSocket);
	}
}

function startServer() {
	var server;

	function logListening() {
		l.log('server listening on ' + conf.protocol + '/' + conf.address + ':' + conf.port);
	}

	if (conf.protocol == "udp4" || conf.protocol == "udp6") {
		server = dgram.createSocket(conf.protocol);

		server.on("message", function (socketBuffer, remote) {
			var data = socketBuffer.toString();
			if (conf.debug) {
				l.log("Accepted message from " + remote);
			}
			processLines(data.split("\n"), true);
		});

		server.on("listening", function() {
			logListening();
		});

		server.bind(conf.port, conf.address);
	}
	else {
		server = net.createServer();

		server.on('connection', function(socket) {
			if (conf.debug) {
				l.log("Accepted connection from " + socket.remoteAddress + ":" + socket.remotePort);
			}

			var buffer = '';

			socket.on('data', function(socketBuffer) {
				var data = socketBuffer.toString();
				buffer += data;
				
				var split = buffer.split("\n");
				processLines(split.slice(0, split.length - 1), true);
				buffer = split[split.length-1];
			});

			socket.on('end', function() {
				if (buffer.length > 0) {
					processLines(buffer.split("\n"), true);
				}
			});
		});

		server.on('listening', function() {
			logListening();
		});

		server.listen(conf.port, conf.address);
	}
}

config.configFile(process.argv[2], function (newConfig) {
	if (conf) {
		l.log("new configuration noticed, but a reload is required to use it.");
		return;
	}

	normalizeConfig(newConfig);
	validateConfig(newConfig);

	conf = newConfig;
	l = new logger.Logger(newConfig.log || {});

	startServer();
});
