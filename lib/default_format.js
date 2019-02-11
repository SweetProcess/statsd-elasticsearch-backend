
/*
* We only need last two parameters for act if they exist.
*
* example:
*
* we get 'tasks.create_action.start'
*
* we cut it to 'create_action.start'
*
* */
function generateSweetAct(actKeys) {
    if (actKeys) {
        var listKeys = actKeys.split('.');
        if (listKeys.length < 3) {
            return actKeys;
        } else {
            var splitPlaces = 2;

            if (["redis", "json"].indexOf(listKeys[listKeys.length - 2]) !== -1) {
                splitPlaces = 3
            }

            var act = listKeys.slice(
                listKeys.length - splitPlaces, listKeys.length
            ).join('.');

            return act;
        }
    } else {
        return actKeys;
    }
}

function generateSweetSubType(actKeys) {
    if (actKeys) {
        var listKeys = actKeys.split('.');
        if (listKeys.length < 2) {
            return actKeys;
        } else {
            var act = listKeys.slice(
                listKeys.length - 1, listKeys.length
            ).join('.');

            return act;
        }
    } else {
        return actKeys;
    }
}

function getQueueInformation(keys) {
    var queue = '';
    for (var i = 0; i < keys.length; i += 1) {
        if (keys[i].indexOf('queue:') !== -1) {
            queue = keys[i].split(':')[1];
            break;
        }
    }
    return queue;
}

function removeQueueInformation(keys) {
    for (var i = 0; i < keys.length; i += 1) {
        if (keys[i].indexOf('queue:') !== -1) {
            keys.splice(i, 1);
        }
    }
}

var counters = function (key, value, ts, bucket) {
    var listKeys = key.split('.');
    var queue = getQueueInformation(listKeys);

    if (queue) {
        listKeys = removeQueueInformation(listKeys);
    }

    var act = listKeys.slice(3, listKeys.length).join('.');

    bucket.push({
		"ns": listKeys[0] || '',
		"grp":listKeys[1] || '',
		"tgt":listKeys[2] || '',
        "queue": queue || '',
		"act":generateSweetAct(act) || '',
        "sub": generateSweetSubType(act) || '',
		"val":value,
		"@timestamp": ts
	});
	return 1;
};

var timers = function (key, series, ts, bucket) {
    var listKeys = key.split('.');
    var queue = getQueueInformation(listKeys);

    if (queue) {
        listKeys = removeQueueInformation(listKeys);
    }
    var act = listKeys.slice(3, listKeys.length).join('.');
    for (keyTimer in series) {
      bucket.push({
		"ns": listKeys[0] || '',
		"grp":listKeys[1] || '',
		"tgt":listKeys[2] || '',
        "queue": queue || '',
		"act":generateSweetAct(act) || '',
        "sub": generateSweetSubType(act) || '',
		"val":series[keyTimer],
		"@timestamp": ts
	 });
    }
	return series.length;
};

var timer_data = function (key, value, ts, bucket) {
    var listKeys = key.split('.');
    var queue = getQueueInformation(listKeys);

    if (queue) {
        listKeys = removeQueueInformation(listKeys);
    }
    var act = listKeys.slice(3, listKeys.length).join('.');
    value["@timestamp"] = ts;
    value["ns"]  = listKeys[0] || '';
    value["grp"] = listKeys[1] || '';
    value["tgt"] = listKeys[2] || '';
    value["queue"] = queue || '';
    value["act"] = generateSweetAct(act) || '';
    value["sub"] = generateSweetSubType(act) || '';
    if (value['histogram']) {
      for (var keyH in value['histogram']) {
        value[keyH] = value['histogram'][keyH];
      }
      delete value['histogram'];
    }
    bucket.push(value);
};

exports.counters   = counters;
exports.timers     = timers;
exports.timer_data = timer_data;
exports.gauges     = counters;
