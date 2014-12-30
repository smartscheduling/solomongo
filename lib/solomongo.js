var getCurrentVersion = function(){
    return db.versions.findAndModify(
        {
            query: {
                versiont: {$gt: 0}
            },
            sort: {version: -1},
            update: {
                $setOnInsert: {version: 1}
            },
            upsert: true,
            new: true
        }
    );
}

var currentVersion = getCurrentVersion().version;

var getSnapshotVersion = function(version) {
    return 1;
};

var beginUpdate = function(){
    var version = getCurrentVersion().version;
    var new_version = version + 1;
    db.versions.insert({version: new_version});
    currentVersion = new_version
};

var mapVersions = function () {
    emit(this._id._id, this);
};

var reduceVersions = function (key, values) {
    return values.sort(function (a, b) {
        return a._id._version - b._id._version;
    });
};

var applyDiffs = function (k, values) {
    var result = {_id: k};
    values.forEach(function (v) {
        if (v._snapshot) {
            result = v._snapshot;
        } else {
            result = apply(result, JSON.parse(v._diff));
        }
    });
    return result;
};

var versionedUpdate = function(collection, query, update_doc, options){
    var oldDocs = db[collection].find(query);
    while (oldDocs.hasNext()) {
        var oldDoc, _id;
        oldDoc = oldDocs.next()
        _id = oldDoc._id; delete oldDoc._id;

        var d = diff(oldDoc, doc);
        if (d) {
            db[collection].history.insert(
                {
                    _id: {_id: _id, _version: oldDoc._version},
                    _diff: JSON.stringify(d)
                }
            );
            if (!options.multi) {
                break;
            }
        } else {
            return;
        }
    }
    update_doc._version = currentVersion;
    return db[collection].update(
        query,
        update_doc,
        update
    );
};

var getVersion = function(collection, query, version){
    var snapshotVersion = getSnapshotVersion(version);
    query['_id._version'] = { $gte: snapshotVersion, $lte: version }
    return db[collection].history.mapReduce(
        mapVersions,
        reduceVersions,
        {
            finalize: applyDiffs,
            out: 'inline',
            query: query,
            sort: { '_id._id': 1, '_id._version': 1 }
        }
    );
};
