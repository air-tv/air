(function (databaseName, commandJson) {
    var runtimeKey = "__airCatalogIndexedDbRuntimeV1";
    var root = globalThis;
    if (!root[runtimeKey]) {
        var STORE_1 = Object.freeze({
            sources: "sources",
            generations: "generations",
            orphanQueue: "orphanQueue",
            catalogRecords: "catalogRecords",
            channelRecords: "channelRecords",
            programmes: "programmes",
            counters: "counters",
        });
        var ALL_STORES_1 = Object.freeze(Object.values(STORE_1));
        var connections_1 = new Map();
        var transactions_1 = new Map();
        var error_1 = function (code) {
            var failure = new Error(code);
            failure.name = "AirIndexedDbError";
            return failure;
        };
        var classify_1 = function (failure) {
            var name = failure && failure.name ? failure.name : "";
            if (name === "QuotaExceededError")
                return "AIR_IDB_QUOTA";
            if (name === "AbortError")
                return "AIR_IDB_ABORT";
            if (name === "InvalidStateError" || name === "SecurityError")
                return "AIR_IDB_UNAVAILABLE";
            return "AIR_IDB_FAILURE";
        };
        var parse_1 = function (value) { return value === undefined ? null : (typeof value === "string" ? JSON.parse(value) : value); };
        var stringify_1 = function (value) { return JSON.stringify(value); };
        var sortableKey_1 = function (value) { return String(value).padStart(16, "0"); };
        var rangeForPrefix_1 = function (prefix) { return IDBKeyRange.bound(prefix, prefix + "\uffff"); };
        var request = function (operation) { return new Promise(function (resolve, reject) {
            operation.onsuccess = function () { return resolve(operation.result); };
            operation.onerror = function () { return reject(error_1(classify_1(operation.error))); };
        }); };
        var open_1 = function (name) {
            var existing = connections_1.get(name);
            if (existing)
                return Promise.resolve(existing);
            if (!root.indexedDB)
                return Promise.reject(error_1("AIR_IDB_UNAVAILABLE"));
            return new Promise(function (resolve, reject) {
                var openRequest;
                try {
                    openRequest = root.indexedDB.open(name, 2);
                }
                catch (failure) {
                    reject(error_1(classify_1(failure)));
                    return;
                }
                openRequest.onupgradeneeded = function (event) {
                    var database = openRequest.result;
                    if (event.oldVersion < 2) {
                        for (var _i = 0, _a = Array.from(database.objectStoreNames); _i < _a.length; _i++) {
                            var store = _a[_i];
                            database.deleteObjectStore(store);
                        }
                        for (var _b = 0, ALL_STORES_2 = ALL_STORES_1; _b < ALL_STORES_2.length; _b++) {
                            var store = ALL_STORES_2[_b];
                            if (store !== STORE_1.catalogRecords && store !== STORE_1.channelRecords)
                                database.createObjectStore(store);
                        }
                        database.createObjectStore(STORE_1.catalogRecords, { keyPath: "recordKey" })
                            .createIndex("orderKey", "orderKey", { unique: true });
                        database.createObjectStore(STORE_1.channelRecords, { keyPath: "recordKey" })
                            .createIndex("orderKey", "orderKey", { unique: true });
                    }
                };
                openRequest.onblocked = function () { return reject(error_1("AIR_IDB_BLOCKED")); };
                openRequest.onerror = function () { return reject(error_1(classify_1(openRequest.error))); };
                openRequest.onsuccess = function () {
                    var database = openRequest.result;
                    database.onversionchange = function () {
                        database.close();
                        connections_1.delete(name);
                    };
                    database.onclose = function () { return connections_1.delete(name); };
                    connections_1.set(name, database);
                    resolve(database);
                };
            });
        };
        var transaction_1 = function (database, stores, mode, operationId, body) {
            return new Promise(function (resolve, reject) {
                var tx;
                try {
                    tx = database.transaction(stores, mode);
                }
                catch (failure) {
                    reject(error_1(classify_1(failure)));
                    return;
                }
                transactions_1.set(operationId, tx);
                var result = null;
                var settled = false;
                var fail = function (code) {
                    if (!tx.__airFailure)
                        tx.__airFailure = code;
                    try {
                        tx.abort();
                    }
                    catch (_) { /* already completed */ }
                };
                tx.oncomplete = function () {
                    transactions_1.delete(operationId);
                    if (!settled) {
                        settled = true;
                        resolve(stringify_1(result));
                    }
                };
                tx.onabort = function () {
                    transactions_1.delete(operationId);
                    if (!settled) {
                        settled = true;
                        reject(error_1(tx.__airFailure || classify_1(tx.error)));
                    }
                };
                tx.onerror = function (event) {
                    if (event)
                        event.preventDefault();
                    fail(classify_1(tx.error));
                };
                var setResult = function (value) { result = value; };
                try {
                    body(tx, setResult, fail);
                }
                catch (failure) {
                    fail(classify_1(failure));
                }
            });
        };
        var getJson_1 = function (store, key, onValue, fail) {
            var getRequest = store.get(key);
            getRequest.onsuccess = function () {
                try {
                    onValue(parse_1(getRequest.result));
                }
                catch (_) {
                    fail("AIR_IDB_CORRUPT");
                }
            };
            getRequest.onerror = function () { return fail(classify_1(getRequest.error)); };
        };
        var requireWritable_1 = function (source, generation, generationRow, fail) {
            if (!source || !generationRow) {
                fail("AIR_IDB_NOT_WRITABLE");
                return false;
            }
            if (source.deleted || generation !== source.nextGeneration - 1) {
                fail("AIR_IDB_STALE");
                return false;
            }
            if (source.activeGeneration === generation) {
                fail("AIR_IDB_ACTIVE_IMMUTABLE");
                return false;
            }
            return true;
        };
        var cursorValues_1 = function (store, range, direction, limit, filter, done, fail) {
            var values = [];
            var cursorRequest = store.openCursor(range, direction);
            cursorRequest.onerror = function () { return fail(classify_1(cursorRequest.error)); };
            cursorRequest.onsuccess = function () {
                var cursor = cursorRequest.result;
                if (!cursor || values.length >= limit) {
                    done(values);
                    return;
                }
                var value;
                try {
                    value = parse_1(cursor.value);
                }
                catch (_) {
                    fail("AIR_IDB_CORRUPT");
                    return;
                }
                if (!filter || filter(value))
                    values.push(value);
                if (values.length >= limit)
                    done(values);
                else
                    cursor.continue();
            };
        };
        var countPrefix_1 = function (store, prefix, done, fail) {
            var countRequest = store.count(rangeForPrefix_1(prefix));
            countRequest.onsuccess = function () { return done(countRequest.result); };
            countRequest.onerror = function () { return fail(classify_1(countRequest.error)); };
        };
        var deletePrefixRows_1 = function (tx, command, generationRow, setResult, fail) {
            var remaining = command.maxRows;
            var removed = 0;
            var tasks = [
                [STORE_1.catalogRecords, command.prefixes.catalogRecord],
                [STORE_1.channelRecords, command.prefixes.channelRecord],
                [STORE_1.programmes, command.prefixes.programme, null, null],
                [STORE_1.counters, command.prefixes.counter, null, null],
            ];
            var taskIndex = 0;
            var runNext = function () {
                if (remaining === 0 || taskIndex >= tasks.length) {
                    finishGeneration();
                    return;
                }
                var task = tasks[taskIndex++];
                var store = tx.objectStore(task[0]);
                var cursorRequest = store.openCursor(rangeForPrefix_1(task[1]));
                cursorRequest.onerror = function () { return fail(classify_1(cursorRequest.error)); };
                cursorRequest.onsuccess = function () {
                    var cursor = cursorRequest.result;
                    if (!cursor || remaining === 0) {
                        runNext();
                        return;
                    }
                    cursor.delete();
                    remaining -= 1;
                    removed += 1;
                    cursor.continue();
                };
            };
            var finishGeneration = function () {
                var checks = tasks.map(function (task) { return tx.objectStore(task[0]).count(rangeForPrefix_1(task[1])); });
                var completed = 0;
                var total = 0;
                var _loop_1 = function (check) {
                    check.onerror = function () { return fail(classify_1(check.error)); };
                    check.onsuccess = function () {
                        total += check.result;
                        completed += 1;
                        if (completed !== checks.length)
                            return;
                        if (total === 0) {
                            tx.objectStore(STORE_1.generations).delete(generationRow.generationKey);
                            tx.objectStore(STORE_1.orphanQueue).delete(command.queueKey);
                        }
                        var queueCount = tx.objectStore(STORE_1.orphanQueue).count();
                        queueCount.onerror = function () { return fail(classify_1(queueCount.error)); };
                        queueCount.onsuccess = function () { return setResult({ removedRows: removed, hasMore: queueCount.result > 0 || total > 0 }); };
                    };
                };
                for (var _i = 0, checks_1 = checks; _i < checks_1.length; _i++) {
                    var check = checks_1[_i];
                    _loop_1(check);
                }
            };
            runNext();
        };
        var execute = function (name, text) {
            var command;
            try {
                command = JSON.parse(text);
            }
            catch (_) {
                throw error_1("AIR_IDB_COMMAND");
            }
            if (command.op === "cancel") {
                var active = transactions_1.get(command.targetOperationId);
                if (active) {
                    active.__airFailure = "AIR_IDB_CANCELLED";
                    try {
                        active.abort();
                    }
                    catch (_) { /* already completed */ }
                }
                return Promise.resolve("null");
            }
            if (command.op === "close") {
                var database = connections_1.get(name);
                if (database)
                    database.close();
                connections_1.delete(name);
                return Promise.resolve("null");
            }
            return open_1(name).then(function (database) {
                switch (command.op) {
                    case "open":
                        return "null";
                    case "begin":
                        return transaction_1(database, [STORE_1.sources, STORE_1.generations, STORE_1.orphanQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var sources = tx.objectStore(STORE_1.sources);
                            getJson_1(sources, command.sourceKey, function (existing) {
                                var source = existing || { activeGeneration: null, nextGeneration: 1, revision: 0, activatedAtMs: null, deleted: false };
                                var generation = source.nextGeneration;
                                if (!Number.isSafeInteger(generation) || generation <= 0) {
                                    fail("AIR_IDB_GENERATION_EXHAUSTED");
                                    return;
                                }
                                var generationComponent = sortableKey_1(generation);
                                var generationKey = command.generationPrefix + generationComponent;
                                var queueKey = command.queuePrefix + generationComponent;
                                source.nextGeneration = generation + 1;
                                source.deleted = false;
                                sources.put(stringify_1(source), command.sourceKey);
                                tx.objectStore(STORE_1.generations).put(stringify_1({
                                    stagedAtMs: command.nowMs,
                                    queueKey: queueKey,
                                    generationKey: generationKey,
                                    sourceComponent: command.sourceComponent,
                                    generationComponent: generationComponent,
                                }), generationKey);
                                tx.objectStore(STORE_1.orphanQueue).put(stringify_1({ generationKey: generationKey }), queueKey);
                                setResult({ generation: generation });
                            }, fail);
                        });
                    case "stageCatalog":
                        return transaction_1(database, [STORE_1.sources, STORE_1.generations, STORE_1.catalogRecords, STORE_1.counters], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var source = null;
                            var generationRow = null;
                            var counter = null;
                            var ready = 0;
                            var advance = function () {
                                ready += 1;
                                if (ready !== 3)
                                    return;
                                if (!requireWritable_1(source, command.generation, generationRow, fail))
                                    return;
                                var order = counter && Number.isSafeInteger(counter.nextOrder) ? counter.nextOrder : 0;
                                var index = 0;
                                var records = tx.objectStore(STORE_1.catalogRecords);
                                var writeNext = function () {
                                    if (index >= command.rows.length) {
                                        tx.objectStore(STORE_1.counters).put(stringify_1({ nextOrder: order }), command.counterKey);
                                        setResult(null);
                                        return;
                                    }
                                    var row = command.rows[index++];
                                    getJson_1(records, row.recordKey, function (old) {
                                        row.sortOrder = order;
                                        row.orderKey = command.orderPrefix + sortableKey_1(order) + "|" + row.entityKey;
                                        order += 1;
                                        records.put(row);
                                        writeNext();
                                    }, fail);
                                };
                                writeNext();
                            };
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (value) { source = value; advance(); }, fail);
                            getJson_1(tx.objectStore(STORE_1.generations), command.generationKey, function (value) { generationRow = value; advance(); }, fail);
                            getJson_1(tx.objectStore(STORE_1.counters), command.counterKey, function (value) { counter = value; advance(); }, fail);
                        });
                    case "stageGuide":
                        return transaction_1(database, [STORE_1.sources, STORE_1.generations, STORE_1.channelRecords, STORE_1.programmes, STORE_1.counters], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var source = null;
                            var generationRow = null;
                            var counter = null;
                            var ready = 0;
                            var advance = function () {
                                ready += 1;
                                if (ready !== 3)
                                    return;
                                if (!requireWritable_1(source, command.generation, generationRow, fail))
                                    return;
                                var order = counter && Number.isSafeInteger(counter.nextOrder) ? counter.nextOrder : 0;
                                var index = 0;
                                var records = tx.objectStore(STORE_1.channelRecords);
                                var writeNextChannel = function () {
                                    if (index >= command.channels.length) {
                                        tx.objectStore(STORE_1.counters).put(stringify_1({ nextOrder: order }), command.counterKey);
                                        for (var _i = 0, _a = command.programmes; _i < _a.length; _i++) {
                                            var programme = _a[_i];
                                            tx.objectStore(STORE_1.programmes).put(stringify_1(programme), programme.recordKey);
                                        }
                                        setResult(null);
                                        return;
                                    }
                                    var row = command.channels[index++];
                                    getJson_1(records, row.recordKey, function (old) {
                                        row.sortOrder = order;
                                        row.orderKey = command.orderPrefix + sortableKey_1(order) + "|" + row.channelKey;
                                        order += 1;
                                        records.put(row);
                                        writeNextChannel();
                                    }, fail);
                                };
                                writeNextChannel();
                            };
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (value) { source = value; advance(); }, fail);
                            getJson_1(tx.objectStore(STORE_1.generations), command.generationKey, function (value) { generationRow = value; advance(); }, fail);
                            getJson_1(tx.objectStore(STORE_1.counters), command.counterKey, function (value) { counter = value; advance(); }, fail);
                        });
                    case "activate":
                        return transaction_1(database, [STORE_1.sources, STORE_1.generations, STORE_1.orphanQueue, STORE_1.catalogRecords, STORE_1.channelRecords, STORE_1.programmes], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var source = null;
                            var generationRow = null;
                            var catalogCount = null;
                            var channelCount = null;
                            var programmeCount = null;
                            var ready = 0;
                            var advance = function () {
                                ready += 1;
                                if (ready !== 5)
                                    return;
                                if (!requireWritable_1(source, command.generation, generationRow, fail))
                                    return;
                                if (catalogCount !== command.expected.catalogItems || channelCount !== command.expected.channels || programmeCount !== command.expected.programmes) {
                                    fail("AIR_IDB_COUNT_MISMATCH");
                                    return;
                                }
                                var oldGeneration = source.activeGeneration;
                                source.activeGeneration = command.generation;
                                source.revision += 1;
                                source.activatedAtMs = command.nowMs;
                                source.deleted = false;
                                tx.objectStore(STORE_1.sources).put(stringify_1(source), command.sourceKey);
                                tx.objectStore(STORE_1.orphanQueue).delete(generationRow.queueKey);
                                if (oldGeneration !== null && oldGeneration !== command.generation) {
                                    var oldKey_1 = command.generationPrefix + sortableKey_1(oldGeneration);
                                    var oldQueueKey_1 = "Q|" + command.nowKey + "|" + command.sourceComponent + "|" + sortableKey_1(oldGeneration);
                                    getJson_1(tx.objectStore(STORE_1.generations), oldKey_1, function (oldRow) {
                                        if (oldRow) {
                                            oldRow.queueKey = oldQueueKey_1;
                                            tx.objectStore(STORE_1.generations).put(stringify_1(oldRow), oldKey_1);
                                            tx.objectStore(STORE_1.orphanQueue).put(stringify_1({ generationKey: oldKey_1 }), oldQueueKey_1);
                                        }
                                    }, fail);
                                }
                                setResult(source);
                            };
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (value) { source = value; advance(); }, fail);
                            getJson_1(tx.objectStore(STORE_1.generations), command.generationKey, function (value) { generationRow = value; advance(); }, fail);
                            countPrefix_1(tx.objectStore(STORE_1.catalogRecords), command.prefixes.catalogRecord, function (value) { catalogCount = value; advance(); }, fail);
                            countPrefix_1(tx.objectStore(STORE_1.channelRecords), command.prefixes.channelRecord, function (value) { channelCount = value; advance(); }, fail);
                            countPrefix_1(tx.objectStore(STORE_1.programmes), command.prefixes.programme, function (value) { programmeCount = value; advance(); }, fail);
                        });
                    case "status":
                        return transaction_1(database, [STORE_1.sources], "readonly", command.operationId, function (tx, setResult, fail) {
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (source) { return setResult(source && !source.deleted ? source : null); }, fail);
                        });
                    case "catalogPage":
                    case "channelPage":
                        return transaction_1(database, [STORE_1.sources, command.op === "catalogPage" ? STORE_1.catalogRecords : STORE_1.channelRecords], "readonly", command.operationId, function (tx, setResult, fail) {
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (source) {
                                if (!source || source.deleted || source.activeGeneration === null) {
                                    setResult([]);
                                    return;
                                }
                                var prefix = command.activePrefix + sortableKey_1(source.activeGeneration) + command.tailPrefix;
                                var lower = command.afterKey === null ? prefix : prefix + command.afterKey + "|\uffff";
                                var range = IDBKeyRange.bound(lower, prefix + "\uffff", command.afterKey !== null, false);
                                cursorValues_1(tx.objectStore(command.op === "catalogPage" ? STORE_1.catalogRecords : STORE_1.channelRecords).index("orderKey"), range, "next", command.limit, null, setResult, fail);
                            }, fail);
                        });
                    case "guideWindow":
                        return transaction_1(database, [STORE_1.sources, STORE_1.programmes], "readonly", command.operationId, function (tx, setResult, fail) {
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (source) {
                                if (!source || source.deleted || source.activeGeneration === null) {
                                    setResult([]);
                                    return;
                                }
                                var prefix = command.activePrefix + sortableKey_1(source.activeGeneration) + command.tailPrefix;
                                var range = IDBKeyRange.bound(prefix, prefix + command.untilKey, false, true);
                                cursorValues_1(tx.objectStore(STORE_1.programmes), range, "next", command.limit, function (row) { return row.endMs > command.fromMs; }, setResult, fail);
                            }, fail);
                        });
                    case "nowNext":
                        return transaction_1(database, [STORE_1.sources, STORE_1.programmes], "readonly", command.operationId, function (tx, setResult, fail) {
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (source) {
                                if (!source || source.deleted || source.activeGeneration === null) {
                                    setResult({ current: null, next: null });
                                    return;
                                }
                                var prefix = command.activePrefix + sortableKey_1(source.activeGeneration) + command.tailPrefix;
                                var current = undefined;
                                var next = undefined;
                                var complete = function () { if (current !== undefined && next !== undefined)
                                    setResult({ current: current, next: next }); };
                                var currentRequest = tx.objectStore(STORE_1.programmes).openCursor(IDBKeyRange.bound(prefix, prefix + command.atKey + "|\uffff"), "prev");
                                currentRequest.onerror = function () { return fail(classify_1(currentRequest.error)); };
                                currentRequest.onsuccess = function () {
                                    var cursor = currentRequest.result;
                                    if (!cursor) {
                                        current = null;
                                        complete();
                                        return;
                                    }
                                    var row;
                                    try {
                                        row = parse_1(cursor.value);
                                    }
                                    catch (_) {
                                        fail("AIR_IDB_CORRUPT");
                                        return;
                                    }
                                    if (row.endMs > command.atMs) {
                                        current = row;
                                        complete();
                                    }
                                    else
                                        cursor.continue();
                                };
                                var nextRequest = tx.objectStore(STORE_1.programmes).openCursor(IDBKeyRange.bound(prefix + command.atKey + "|\uffff", prefix + "\uffff", true, false), "next");
                                nextRequest.onerror = function () { return fail(classify_1(nextRequest.error)); };
                                nextRequest.onsuccess = function () {
                                    var cursor = nextRequest.result;
                                    if (!cursor) {
                                        next = null;
                                        complete();
                                        return;
                                    }
                                    try {
                                        next = parse_1(cursor.value);
                                    }
                                    catch (_) {
                                        fail("AIR_IDB_CORRUPT");
                                        return;
                                    }
                                    complete();
                                };
                            }, fail);
                        });
                    case "deleteSource":
                        return transaction_1(database, [STORE_1.sources, STORE_1.generations, STORE_1.orphanQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            getJson_1(tx.objectStore(STORE_1.sources), command.sourceKey, function (source) {
                                if (!source || source.deleted) {
                                    setResult(null);
                                    return;
                                }
                                var active = source.activeGeneration;
                                source.activeGeneration = null;
                                source.revision += 1;
                                source.deleted = true;
                                tx.objectStore(STORE_1.sources).put(stringify_1(source), command.sourceKey);
                                if (active !== null) {
                                    var generationKey_1 = command.generationPrefix + sortableKey_1(active);
                                    var queueKey_1 = "Q|" + command.nowKey + "|" + command.sourceComponent + "|" + sortableKey_1(active);
                                    getJson_1(tx.objectStore(STORE_1.generations), generationKey_1, function (row) {
                                        if (row) {
                                            row.queueKey = queueKey_1;
                                            tx.objectStore(STORE_1.generations).put(stringify_1(row), generationKey_1);
                                            tx.objectStore(STORE_1.orphanQueue).put(stringify_1({ generationKey: generationKey_1 }), queueKey_1);
                                        }
                                    }, fail);
                                }
                                setResult(null);
                            }, fail);
                        });
                    case "cleanup":
                        return transaction_1(database, ALL_STORES_1, "readwrite", command.operationId, function (tx, setResult, fail) {
                            var cursorRequest = tx.objectStore(STORE_1.orphanQueue).openCursor();
                            cursorRequest.onerror = function () { return fail(classify_1(cursorRequest.error)); };
                            cursorRequest.onsuccess = function () {
                                var cursor = cursorRequest.result;
                                if (!cursor) {
                                    setResult({ removedRows: 0, hasMore: false });
                                    return;
                                }
                                var row;
                                try {
                                    row = parse_1(cursor.value);
                                }
                                catch (_) {
                                    fail("AIR_IDB_CORRUPT");
                                    return;
                                }
                                command.queueKey = cursor.key;
                                getJson_1(tx.objectStore(STORE_1.generations), row.generationKey, function (generationRow) {
                                    if (!generationRow) {
                                        cursor.delete();
                                        setResult({ removedRows: 0, hasMore: true });
                                        return;
                                    }
                                    command.prefixes = {
                                        catalogRecord: "I|" + generationRow.sourceComponent + "|" + generationRow.generationComponent + "|",
                                        channelRecord: "J|" + generationRow.sourceComponent + "|" + generationRow.generationComponent + "|",
                                        programme: "P|" + generationRow.sourceComponent + "|" + generationRow.generationComponent + "|",
                                        counter: "K|" + generationRow.sourceComponent + "|" + generationRow.generationComponent + "|",
                                    };
                                    deletePrefixRows_1(tx, command, generationRow, setResult, fail);
                                }, fail);
                            };
                        });
                    default:
                        throw error_1("AIR_IDB_COMMAND");
                }
            });
        };
        root[runtimeKey] = { execute: execute };
    }
    return root[runtimeKey].execute(databaseName, commandJson);
});
