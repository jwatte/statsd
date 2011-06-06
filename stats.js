var dgram  = require('dgram')
, sys    = require('sys')
, net    = require('net')
, configMod = require('./config')

var counters = {};
var stats = {};
var timers = {};
var debugInt, flushInt, server;
var flushInterval = 10000;
var config = {};

function onDatagram(data, rinfo) {
    data = data.toString();
    if (config.dumpMessages) {
        sys.log(data);
    }
    var msgSplit = data.split('\n');
    for (var msgIndex in msgSplit) {
        var msg = msgSplit[msgIndex].trim();
        if (!msg) {
            continue;
        }
        var bits = msg.split(':');
        var key = bits.shift()
            .replace(/\s+/g, '_')
            .replace(/\//g, '-')
            .replace(/[^a-zA-Z_\-0-9\.]/g, '');

        if (!key) {
            sys.log('Bad line (no key): ' + key);
            continue;
        }
        if (bits.length == 0) {
            sys.log('Bad line (no data): ' + msg);
            continue;
        }

        for (var i = 0; i < bits.length; i++) {
            var sampleRate = 1;
            var fields = bits[i].split("|");
            if (fields[1] === undefined) {
                sys.log('Bad line (no unit): ' + fields);
                continue;
            }
            var unit = fields[1].trim();
            if (unit == "ms") {
                if (! timers[key]) {
                    timers[key] = [];
                }
                timers[key].push(Number(fields[0] || 0) * 0.001);
            } else if (unit == "s") {
                if (! timers[key]) {
                    timers[key] = [];
                }
                timers[key].push(Number(fields[0] || 0));
            }
            else if (unit == "" || unit == "%" || unit == "/s") {
                val = Number(fields[0] || 1);
                if (! stats[key]) {
                    stats[key] = {
                        min: val,
                        max: val,
                        n: 1,
                        sum: val,
                        sumSq: val*val
                    };
                }
                else{
                    obj = stats[key];
                    obj.min = Math.min(obj.min, val);
                    obj.max = Math.max(obj.max, val);
                    obj.n = obj.n + 1;
                    obj.sum = obj.sum + val;
                    obj.sumSq = obj.sumSq + val;
                }
            } else if (unit == "c") {
                if (fields[2]) {
                    sampleRate = 0;
                    if (fields[2].match(/^@([\d\.]+)/)) {
                        sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
                    }
                    if (sampleRate <= 0) {
                        sys.log('Bad rate: ' + fields);
                        continue;
                    }
                }
                if (! counters[key]) {
                    counters[key] = 0;
                }
                counters[key] += Number(fields[0] || 1) * (1 / sampleRate);
            }
            else {
                sys.log("Unknown counter unit: " + unit);
            }
        }
    }
}

function flushFunction() {
    var statString = '';
    var ts = Math.floor(new Date().getTime() / 1000);
    var numStats = 0;
    var key;

    for (key in counters) {
        var value = counters[key] / (flushInterval / 1000);
        var message = 'stats.' + key + ' ' + value + ' ' + ts + "\n";
        message += 'stats_counts.' + key + ' ' + counters[key] + ' ' + ts + "\n";
        statString += message;

        numStats += 1;
    }
    counters = {};

    for (key in stats) {
        var obj = stats[key];
        var message = 'stats.' + key + ".avg" + ' ' + (obj.sum / obj.n) + " " + ts + "\n";
        message += 'stats.' + key + ".min" + ' ' + obj.min + " " + ts + "\n";
        message += 'stats.' + key + ".max" + ' ' + obj.max + " " + ts + "\n";
        message += 'stats_counts.' + key + " " + obj.sum + " " + ts + "\n";
        if (obj.n > 2) {
            var sdev = Math.sqrt((obj.n * obj.sumSq - obj.sum * obj.sum) / (obj.n * (obj.n - 1)));
            message += "stats." + key + ".sdev" + " " + sdev + " " + ts + "\n";
        }
        statString += message;

        numStats += 1;
    }
    stats = {};

    for (key in timers) {
        if (timers[key].length > 0) {
            var pctThreshold = config.percentThreshold || 90;
            var values = timers[key].sort(function (a,b) { return a-b; });
            var count = values.length;
            var min = values[0];
            var max = values[count - 1];

            var mean = min;
            var maxAtThreshold = max;

            if (count > 1) {
                var thresholdIndex = Math.round(((100 - pctThreshold) / 100) * count);
                var numInThreshold = count - thresholdIndex;
                values = values.slice(0, numInThreshold);
                maxAtThreshold = values[numInThreshold - 1];

                // average the remaining timings
                var sum = 0;
                for (var i = 0; i < numInThreshold; i++) {
                    sum += values[i];
                }

                mean = sum / numInThreshold;
            }

            var message = "";
            message += 'stats.timers.' + key + '.mean ' + mean + ' ' + ts + "\n";
            message += 'stats.timers.' + key + '.upper ' + max + ' ' + ts + "\n";
            message += 'stats.timers.' + key + '.upper_' + pctThreshold + ' ' + maxAtThreshold + ' ' + ts + "\n";
            message += 'stats.timers.' + key + '.lower ' + min + ' ' + ts + "\n";
            message += 'stats.timers.' + key + '.count ' + count + ' ' + ts + "\n";
            statString += message;

            numStats += 1;
        }
        timers = {};
    }

    statString += 'statsd.numStats ' + numStats + ' ' + ts + "\n";
    if (config.debug) {
        sys.log('sending stats: ' + statString);
    }

    try {
        var graphite = net.createConnection(config.graphitePort, config.graphiteHost);
        graphite.addListener('error', function(connectionException){
                    sys.log("connectionException: " + connectionException + "; graphitePort: " +
                        config.graphitePort + "; graphiteHost: " + config.graphiteHost);
                });
        graphite.on('connect', function() {
                    this.write(statString);
                    this.end();
                });
    } catch(e){
        sys.log("createConnection: " + e);
    }
}

function setupAfterConfig(newConfig, oldConfig) {
    if (! newConfig.debug && debugInt) {
        clearInterval(debugInt); 
        debugInt = undefined;
    }

    if (newConfig.debug) {
        if (debugInt !== undefined) { clearInterval(debugInt); }
        debugInt = setInterval(function () { 
                    sys.log("Counters:\n" + sys.inspect(counters) + "\nTimers:\n" + sys.inspect(timers));
                }, newConfig.debugInterval || 10000);
    }

    config = newConfig;
    sys.log("New config: " + JSON.stringify(config));

    if (server === undefined) {
        server = dgram.createSocket('udp4', onDatagram);

        server.bind(config.port || 8125);

        flushInterval = Number(config.flushInterval || 10000);

        flushInt = setInterval(flushFunction, flushInterval);
    }
}

configMod.configFile(process.argv[2], setupAfterConfig);

