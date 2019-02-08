
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
        if (listKeys.length < 2) {
            return actKeys;
        } else {
            var act = listKeys.slice(
                listKeys.length - 2, listKeys.length
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

var counters = function (key, value, ts, bucket) {
    var listKeys = key.split('.');
    var act = listKeys.slice(3, listKeys.length).join('.');

    bucket.push({
		"ns": listKeys[0] || '',
		"grp":listKeys[1] || '',
		"tgt":listKeys[2] || '',
		"act":generateSweetAct(act) || '',
        "sub": generateSweetSubType(act) || '',
		"val":value,
		"@timestamp": ts
	});
	return 1;
};

var timers = function (key, series, ts, bucket) {
    var listKeys = key.split('.');
    var act = listKeys.slice(3, listKeys.length).join('.');
    for (keyTimer in series) {
      bucket.push({
		"ns": listKeys[0] || '',
		"grp":listKeys[1] || '',
		"tgt":listKeys[2] || '',
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
    var act = listKeys.slice(3, listKeys.length).join('.');
    value["@timestamp"] = ts;
    value["ns"]  = listKeys[0] || '';
    value["grp"] = listKeys[1] || '';
    value["tgt"] = listKeys[2] || '';
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
