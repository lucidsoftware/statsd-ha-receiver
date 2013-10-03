var dgram  = require('dgram'),
    util   = require('util'),
    net    = require('net'),
    config = require('./lib/config'),
    fs     = require('fs'),
    events = require('events'),
    set    = require('./lib/set'),
    distributor = require('./lib/distributor'),
    logger = require('./lib/logger');

var startupTime = Math.round(new Date().getTime() / 1000);
var conf;
var l;
var d;

var counters = {};
var timers = {};
var timerCounters = {};
var gauges = {};
var sets = {};

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

function flushMetrics() {
	if (conf.debug) {
		l.log("flushing");
	}

	var metrics = {
		counters: counters,
		timers: timers,
		timerCounters: timerCounters,
		gauges: gauges,
		sets: sets
	};

	counters = {};
	timers = {};
	timerCounters = {};
	gauges = {};
	sets = {};

	d.process(metrics);
}

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

function recursiveAggregatedKeys(key, type, found, levels) {
	var aggregationIndex, backIndex, generationIndex;

	if (levels >= 20) {
		l.log("Too many levels of aggregation with " + key);
		return;
	}

	if (found.hasOwnProperty(key)) {
		l.log("Recursive aggregation found with " + key);
		return;
	}

	found[key] = true;
	for (aggregationIndex = 0; aggregationIndex < conf.aggregation.length; aggregationIndex++) {
		var rule = conf.aggregation[aggregationIndex];

		if (rule.types != '*' && rule.types.indexOf(type) == -1) { continue; }

		var matches = key.match(rule.match);
		if (!matches) { continue; }

		for (generationIndex = 0; generationIndex < rule.generate.length; generationIndex++) {
			var newKey = rule.generate[generationIndex];
			for (backIndex = 1; backIndex < matches.length; backIndex++) {
				var replace = new RegExp("\\(" + backIndex + "\\)", "g");
				newKey = newKey.replace(replace, matches[backIndex]);
			}

			if (rule.recursive) {
				recursiveAggregatedKeys(newKey, type, found, levels + 1);
			}
		}

		if (rule.last) { break; }
	}
}

function aggregatedKeys(key, type, includeOriginal) {
	var key;
	includeOriginal = includeOriginal || false;

	var found = {};

	recursiveAggregatedKeys(key, type, found, 0);
	if (!includeOriginal) {
		delete(found[key]);
	}

	var ret = [];
	for (key in found) {
		ret.push(key);
	}

	return ret;
}

function processLine(line) {
	var metricType;

	line = line.trim();
	if (line == '') return;

	if (conf.dumpMessages) {
		l.log(line);
	}

	var bits = line.split(':');
	var key = bits.shift()
	          .replace(/\s+/g, '_')
	          .replace(/\//g, '-')
	          .replace(/[^a-zA-Z_\-0-9\.]/g, '');

	if (bits.length === 0) {
		bits.push("1");
	}

	var keyAggregationsByMetricType = {};

	for (var i = 0; i < bits.length; i++) {
		var sampleRate = 1;
		var fields = bits[i].split("|");

		if (fields[2]) {
			if (fields[2].match(/^@([\d\.]+)/)) {
				sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
			} else {
				l.log('Bad line: ' + fields + ' in msg "' + line +'"; has invalid sample rate');
				// counters[bad_lines_seen]++;
				continue;
			}
		}

		if (fields[1] === undefined) {
			l.log('Bad line: ' + fields + ' in msg "' + line +'"');
			// counters[bad_lines_seen]++;
			continue;
		}

		var metricType = fields[1].trim();

		if (!keyAggregationsByMetricType.hasOwnProperty(metricType)) {
			keyAggregationsByMetricType[metricType] = [key].concat(aggregatedKeys(key, metricType));
		}

		var allKeys = keyAggregationsByMetricType[metricType];
		for (var allKeyIndex = 0; allKeyIndex < allKeys.length; allKeyIndex++) {
			var oneKey = allKeys[allKeyIndex];
			if (metricType === "ms") {
				if (! timers[oneKey]) {
					timers[oneKey] = [];
					timerCounters[oneKey] = 0;
				}
				timers[oneKey].push(Number(fields[0] || 0));
				timerCounters[oneKey] += (1 / sampleRate);
			} else if (metricType === "g") {
				if (gauges[oneKey] && fields[0].match(/^[-+]/)) {
					gauges[oneKey] += Number(fields[0] || 0);
				} else {
					gauges[oneKey] = Number(fields[0] || 0);
				}
			} else if (metricType === "s") {
				if (! sets[oneKey]) {
					sets[oneKey] = new set.Set();
				}
				sets[oneKey].insert(fields[0] || '0');
			} else {
				if (! counters[oneKey]) {
					counters[oneKey] = 0;
				}
				counters[oneKey] += Number(fields[0] || 1) * (1 / sampleRate);
			}
		}
	}
}

function processLines(lines) {
	for(var i = 0; i < lines.length; i++) {
		processLine(lines[i]);
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
			processLines(data.split("\n"));
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
				buffer += socketBuffer.toString();
			});

			socket.on('end', function() {
				if (buffer.length > 0) {
					processLines(buffer.split("\n"));
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
	d = new distributor.Distributor(l, conf);

	setInterval(flushMetrics, conf.flushInterval);

	startServer();
});
