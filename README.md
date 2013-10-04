statsd-ha-receiver
==================

Simple daemon that uses the [StatsD](https://github.com/etsy/statsd) protocol to aggregate and distribute stats among StatsD servers. This helps in creating a highly available StatsD / Graphite architecture.

Installation and Configuration
==================

 * Install node.js
 * Clone the project
 * Create a config file from exampleConfig.js and put it somewhere
 * Start the Daemon:

    node stats.js /path/to/config
