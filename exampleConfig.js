/*

Required Variables:

	shardRingSize:    size of the hash ring for sharding
	shards:           an array of shards to send stats to. The order of the
	                  shards must be the same on all statsd-ha-receivers, or
	                  metrics will be sent to the wrong shards.
		hostname:     hostname or ip of the shard
		port:         port of the shard
		protocol:     udp4, udp6, tcp4, tcp6
		ringIndex:    where on the hash ring this node lies (0-based)

Optional Variables:

	port:             Port to listen on [default: 8125]
	address:          IP Address to listen on [default: 0.0.0.0]
	protocol:         udp4, udp6, tcp4, tcp6 [default: tcp4]

	flushInterval:    interval (in ms) to flush to Graphite. [default: 10000]

	debug:            debug flag [default: false]
	dumpMessages:     log all incoming messages [default: false]
	prefix:           prefix for all incoming stats [default: '']

	replication:      number of shards to send each stat to [default: 1]

	batchSize:        max packet size for udp [default: 1024]

	log:              log settings [object, default: undefined]
		backend:      where to log: stdout or syslog [string, default: stdout]
		application:  name of the application for syslog [string, default: statsd]
		level:        log level for [node-]syslog [string, default: LOG_INFO]

	aggregation:      array of aggregation rules to apply to incoming and aggregated metrics.
	                  Metrics [default: [] (empty array)]
		match:        regex to match incoming metrics against
		types:        array of gauge, set, counter, and/or timer.
		recursive:    true if the generated metrics should also be aggregated
		last:         true if processing should stop if a metric matches this rule
		generate:     array of metrics to generate when an incoming metric matches
		              this rule. These may use backreferences in the form (1), (2)...

*/

{
	shardRingSize: 100,
	shards: [
		{ hostname: '1.1.1.1', protocol: 'udp4', port: 8125, ringIndex:  0 },
		{ hostname: '2.2.2.2', protocol: 'udp4', port: 8125, ringIndex: 25 },
		{ hostname: '3.3.3.3', protocol: 'udp4', port: 8125, ringIndex: 50 },
		{ hostname: '4.4.4.4', protocol: 'udp4', port: 8125, ringIndex: 75 },
	],
	aggregation: [
		{
			match: /^(stats\.counters\.servers)\.myserver\.(.*)$/,
			types: ['counter'],
			last: false,
			recursive: false,
			generate: [
				"(1).allservers.(2)",
				"generated.(2)"
			]
		}, {
			match: /^(stats\.gauges)\.applications\.lucidchart\.api\.([^\.]+)\.current$/,
			types: ['gauge'],
			recursive: false,
			last: true,
			generate: [
				"lucidchart.api-(2).current"
			]
		}
	]
}
