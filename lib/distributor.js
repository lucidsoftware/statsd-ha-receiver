/**
 * Stat distributor
 *
 * Distributes stats to different shards of statsd instances
 */

var util = require('util'),
    dgram = require('dgram'),
    net = require('net'),
    logger = require('../lib/logger');


function Distributor(logger, config) {
	this.logger = logger;
	this.config = config;

	this.batchSize = config.batchSize || 1024;
	this.blacklist = config.blacklist || [];
	this.shards = config.shards.slice().sort(function (a, b) { return a.ringIndex - b.ringIndex; });
	this.shardRingSize = config.shardRingSize;
	this.replicate = Math.min(this.shards.length, this.config.replicate);
};

Distributor.prototype.hashCode = function(str) {
	var hashCode = 0;
	var i;

	for (i = 0; i < str.length; i++) {
		char = str.charCodeAt(i);
		hashCode = ((hashCode << 5) - hashCode) + char;
		hashCode = hashCode & hashCode;
	}

	return hashCode;
}

Distributor.prototype.blacklisted = function(key) {
	var i;

	for (i = 0; i < this.blacklist.length; i++) {
		if (this.blacklist[i].test(key)) {
			return true;
		}
	}

	return false;
}

Distributor.prototype.sampleRateToString = function(number) {
	var string = number.toFixed(3);
	var index = string.length - 1;

	if (string[index] == "0") {
		while (index >= 0 && (string[index] == "0" || string[index] == ".")) { index--; }
		if (index < 0) {
			string = '0';
		}
		else {
			string = string.substring(0, index + 1);
		}
	}

	return string;
};

Distributor.prototype.reconstituteMessages = function(metrics) {
	var key, i;
	var outgoing = [];

	for (key in metrics.gauges) {
		if (this.blacklisted(key)) { continue; }
		outgoing.push(key + ":" + metrics.gauges[key] + "|g");
	}

	for (key in metrics.counters) {
		if (this.blacklisted(key)) { continue; }
		outgoing.push(key + ":" + metrics.counters[key] + "|c");
	}

	for (key in metrics.timers) {
		if (this.blacklisted(key)) { continue; }
		var values = metrics.timers[key];
		var sampleRate = values.length / metrics.timerCounters[key];
		var sampleRateString = (sampleRate >= 1) ? "" : ("|@" + this.sampleRateToString(sampleRate));

		var rebuiltValues = [];
		for (i = 0; i < values.length; i++) {
			rebuiltValues.push(values[i] + "|ms" + sampleRateString);
		}

		outgoing.push(key + ":" + rebuiltValues.join(":"));
	}

	for (key in metrics.sets) {
		if (this.blacklisted(key)) { continue; }
		var values = metrics.sets[key].values();

		var rebuiltValues = [];
		for (i = 0; i < values.length; i++) {
			rebuiltValues.push(values[i] + "|s");
		}

		outgoing.push(key + ":" + rebuiltValues.join(":"));
	}

	return outgoing;
};

Distributor.prototype.splitStats = function(stats) {
	var i;

	var buffers = [];
	var buffer = [];
	var bufferLength = 0;

	for (i = 0; i < stats.length; i++) {
		var line = stats[i];
		var lineLength = line.length;

		if (bufferLength != 0 && (bufferLength + lineLength) > this.batchSize) {
			buffers.push(buffer);
			buffer = [];
			bufferLength = 0;
		}

		buffer.push(line);
		bufferLength += lineLength;
	}

	if (bufferLength > 0) {
		buffers.push(buffer);
	}

	var lines = [];
	for (i = 0; i < buffers.length; i++) {
		lines.push(buffers[i].join("\n"));
	}

	return lines;
}

Distributor.prototype.assignShards = function(metrics) {
	var i, j;
	var metricsInShardIndex = [];

	for (i = 0; i < this.shards.length; i++) {
		metricsInShardIndex.push([]);
	}

	for (i = 0; i < metrics.length; i++) {
		var shardIndex;
		if (this.replicate == this.shards.length) {
			shardIndex = 0;
		}
		else {
			var key = metrics[i].split(":")[0];
			var ringIndex = Math.abs(this.hashCode(key)) % this.shardRingSize;

			for (j = this.shards.length - 1; j >= 0; j--) {
				if (ringIndex >= this.shards[j].ringIndex) {
					shardIndex = j;
					break;
				}
			}
		}

		for (var rep = 0; rep < this.replicate; rep++) {
			metricsInShardIndex[shardIndex].push(metrics[i]);
			shardIndex = (shardIndex + 1) % this.shards.length;
		}
	}

	return metricsInShardIndex;
};

Distributor.prototype.sendToHost = function(shard, metrics) {
	var i;
	var data = this.splitStats(metrics);

	try {
		if (shard.protocol == "udp4" || shard.protocol == "udp6") {
			var sock = dgram.createSocket(shard.protocol);
			for (i = 0; i < data.length; i++) {
				var single = data[i];
				var buffer = new Buffer(single);
				sock.send(buffer, 0, single.length, shard.port, shard.hostname, function(err, bytes) {
					if (err && debug) {
						l.log(err);
					}
				});
			}
		}
		else {
			var connection = net.createConnection(shard.port, shard.hostname);
			connection.addListener('error', function(connectionException) {
				if (debug) {
					l.log(connectionException);
				}
			});
			connection.on('connect', function() {
				for (i = 0; i < data.length; i++) {
					var single = data[i];
					if (i != 0) {
						this.write("\n");
					}
					this.write(single);
				}
				this.end();
			});
		}
	}
	catch(e) {
		if (debug) {
			l.log(e);
		}
	}
};

Distributor.prototype.distribute = function(metricsByHostIndex) {
	for (var i = 0; i < this.shards.length; i++) {
		var shard = this.shards[i];
		var metrics = metricsByHostIndex[i];
		if (metrics.length > 0) {
			this.sendToHost(shard, metrics);
		}
	}
};

Distributor.prototype.process = function(metrics) {
	this.distribute(this.assignShards(this.reconstituteMessages(metrics)));
};

exports.Distributor = Distributor;
