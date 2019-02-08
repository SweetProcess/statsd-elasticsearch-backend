/*
 * Flush stats to ElasticSearch (http://www.elasticsearch.org/)
 *
 * To enable this backend, include 'elastic' in the backends
 * configuration array:
 *
 *   backends: ['./backends/elastic'] 
 *  (if the config file is in the statsd folder)
 *
 * A sample configuration can be found in exampleElasticConfig.js
 *
 * This backend supports the following config options:
 *
 *   host:            hostname or IP of ElasticSearch server
 *   port:            port of Elastic Search Server
 *   path:            http path of Elastic Search Server (default: '/')
 *   indexPrefix:     Prefix of the dynamic index to be created (default: 'statsd')
 *   indexTimestamp:  Timestamping format of the index, either "year", "month" or "day"
 *   indexType:       The dociment type of the saved stat (default: 'stat')
 */

let net = require('net'),
    util = require('util'),
    http = require('http');
// this will be instantiated to the logger
let lg;
let debug;
let flushInterval;
let elasticHost;
let elasticPort;
let elasticPath;
let elasticIndex;
let elasticIndexTimestamp;
let elasticCountType;
let elasticTimerType;
let elasticTimerDataType;
let elasticGaugeDataType;
let elasticFormatter;
let elasticUsername;
let elasticPassword;
let elasticFromHost;

let elasticStats = {};


function reformatData(items, type, index) {
    let payload = '';
    for (let key in items) {
        payload += '{"index":{"_index":"' + index + '","_type" : "_doc"}}'+"\n";
        payload += '{';
        let innerPayload = '';
        for (let statKey in items[key]){
            if (innerPayload) innerPayload += ',';
            innerPayload += '"'+statKey+'":"'+items[key][statKey]+'"';
        }
        innerPayload += ',"host":"' + elasticFromHost + '"';
        innerPayload += ',"message_type":"' + type + '"';
        payload += innerPayload +'}'+"\n";
    }
    return payload;
}

function es_bulk_insert(listCounters, listTimers, listTimerData, listGaugeData) {

    let indexDate = new Date();

    let statsdIndex = elasticIndex + '-' + indexDate.getUTCFullYear()

    if (elasticIndexTimestamp == 'month' || elasticIndexTimestamp == 'day'){
        let indexMo = indexDate.getUTCMonth() +1;
        if (indexMo < 10) {
            indexMo = '0'+indexMo;
        }
        statsdIndex += '.' + indexMo;
    }

    if (elasticIndexTimestamp == 'day'){
        let indexDt = indexDate.getUTCDate();
        if (indexDt < 10) {
            indexDt = '0'+indexDt;
        }
        statsdIndex += '.' +  indexDt;
    }

    let payload = reformatData(listCounters, elasticCountType, statsdIndex);
    payload += reformatData(listTimers, elasticTimerType, statsdIndex);
    payload += reformatData(listTimerData, elasticTimerDataType, statsdIndex);
    payload += reformatData(listGaugeData, elasticGaugeDataType, statsdIndex);

    let optionsPost = {
        host: elasticHost,
        port: elasticPort,
        path: elasticPath + statsdIndex + '/_doc/_bulk',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': payload.length
        }
    };

    if(elasticUsername && elasticPassword) {
        optionsPost.auth = elasticUsername + ':' + elasticPassword;
    }

    let req = http.request(optionsPost, function(res) {
        res.on('data', function(d) {
            if (Math.floor(res.statusCode / 100) == 5){
                let errdata = "HTTP " + res.statusCode + ": " + d;
                lg.log('error', errdata);
            }
        });
    }).on('error', function(err) {
        lg.log('error', 'Error with HTTP request, no stats flushed.');
        console.log(err);
    });

    if (debug) {
        lg.log('ES payload:');
        lg.log(payload);
    }
    req.write(payload);
    req.end();
}

const flush_stats = function elastic_flush(ts, metrics) {
    let statString = '';
    let numStats = 0;
    let key;
    let array_counts = new Array();
    let array_timers = new Array();
    let array_timer_data = new Array();
    let array_gauges = new Array();

    ts = ts*1000;
    /*
      let gauges = metrics.gauges;
      let pctThreshold = metrics.pctThreshold;
    */
    for (key in metrics.counters) {
        numStats += fm.counters(key, metrics.counters[key], ts, array_counts);
    }

    for (key in metrics.timers) {
        numStats += fm.timers(key, metrics.timers[key], ts, array_timers);
    }

    if (array_timers.length > 0) {
        for (key in metrics.timer_data) {
            fm.timer_data(key, metrics.timer_data[key], ts, array_timer_data);
        }
    }

    for (key in metrics.gauges) {
        numStats += fm.gauges(key, metrics.gauges[key], ts, array_gauges);
    }
    if (debug) {
        lg.log('metrics:');
        lg.log( JSON.stringify(metrics) );
    }

    es_bulk_insert(array_counts, array_timers, array_timer_data, array_gauges);

    if (debug) {
        lg.log("debug", "flushed " + numStats + " stats to ES");
    }
};

let elastic_backend_status = function (writeCb) {
    for (stat in elasticStats) {
        writeCb(null, 'elastic', stat, elasticStats[stat]);
    }
};

exports.init = function elasticsearch_init(startup_time, config, events, logger) {
    debug = config.debug;
    lg = logger;

    let configEs = config.elasticsearch || { };

    elasticHost           = configEs.host           || 'localhost';
    elasticPort           = configEs.port           || 9200;
    elasticPath           = configEs.path           || '/';
    elasticIndex          = configEs.indexPrefix    || 'statsd';
    elasticIndexTimestamp = configEs.indexTimestamp || 'day';
    elasticCountType      = configEs.countType      || 'counter';
    elasticTimerType      = configEs.timerType      || 'timer';
    elasticTimerDataType  = configEs.timerDataType  || elasticTimerType + '_stats';
    elasticGaugeDataType  = configEs.gaugeDataType  || 'gauge';
    elasticFormatter      = configEs.formatter      || 'default_format';
    elasticUsername       = configEs.username       || undefined;
    elasticPassword       = configEs.password       || undefined;
    elasticFromHost       = configEs.fromHost       || '';

    fm = require('./' + elasticFormatter + '.js');
    if (debug) {
        lg.log("debug", "loaded formatter " + elasticFormatter);
    }

    if (fm.init) {
        fm.init(configEs);
    }
    flushInterval = config.flushInterval;

    elasticStats.last_flush = startup_time;
    elasticStats.last_exception = startup_time;

    events.on('flush', flush_stats);
    events.on('status', elastic_backend_status);
    return true;
};

