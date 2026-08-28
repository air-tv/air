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
            guideStates: "guideStates",
            guideGenerations: "guideGenerations",
            guideChannels: "guideChannels",
            guideProgrammes: "guideProgrammes",
            guideTimeline: "guideTimeline",
            guideMigration: "guideMigration",
            guideLeases: "guideLeases",
            guideCleanupQueue: "guideCleanupQueue",
        });
        var ALL_STORES_1 = Object.freeze(Object.values(STORE_1));
        var MEDIA_STORES_1 = Object.freeze([
            STORE_1.sources,
            STORE_1.generations,
            STORE_1.orphanQueue,
            STORE_1.catalogRecords,
            STORE_1.channelRecords,
            STORE_1.programmes,
            STORE_1.counters,
        ]);
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
                var blocked = false;
                try {
                    openRequest = root.indexedDB.open(name, 4);
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
                        for (var _b = 0, MEDIA_STORES_2 = MEDIA_STORES_1; _b < MEDIA_STORES_2.length; _b++) {
                            var store = MEDIA_STORES_2[_b];
                            if (store !== STORE_1.catalogRecords && store !== STORE_1.channelRecords)
                                database.createObjectStore(store);
                        }
                        database.createObjectStore(STORE_1.catalogRecords, { keyPath: "recordKey" })
                            .createIndex("orderKey", "orderKey", { unique: true });
                        database.createObjectStore(STORE_1.channelRecords, { keyPath: "recordKey" })
                            .createIndex("orderKey", "orderKey", { unique: true });
                    }
                    if (event.oldVersion < 3) {
                        if (!database.objectStoreNames.contains(STORE_1.guideStates)) {
                            database.createObjectStore(STORE_1.guideStates, { keyPath: "key" })
                                .createIndex("activeFeedKey", "activeFeedKey", { unique: true });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideGenerations)) {
                            var generations = database.createObjectStore(STORE_1.guideGenerations, { keyPath: "key" });
                            generations.createIndex("sourceEpochKey", "sourceEpochKey", { unique: false });
                            generations.createIndex("sourceFeedGeneration", ["sourceKey", "feedId", "generation"], { unique: true });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideChannels)) {
                            database.createObjectStore(STORE_1.guideChannels, { keyPath: "key" })
                                .createIndex("generationChannel", ["generationKey", "channelKey"], { unique: true });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideProgrammes)) {
                            var programmes = database.createObjectStore(STORE_1.guideProgrammes, { keyPath: "key" });
                            programmes.createIndex("endKey", "endKey", { unique: true });
                            programmes.createIndex("generationChannelStart", ["generationKey", "channelKey", "startMs"], { unique: true });
                            programmes.createIndex("generationChannelEffectiveEnd", ["generationKey", "channelKey", "effectiveEndMs", "startMs"], { unique: true });
                            programmes.createIndex("generationLocator", ["generationKey", "key"], { unique: true });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideLeases)) {
                            var leases = database.createObjectStore(STORE_1.guideLeases, { keyPath: "key" });
                            leases.createIndex("generationKey", "generationKey", { unique: false });
                            leases.createIndex("expiresAt", "expiresAt", { unique: false });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideCleanupQueue)) {
                            database.createObjectStore(STORE_1.guideCleanupQueue, { keyPath: "key" })
                                .createIndex("cleanupAt", "cleanupAt", { unique: false });
                        }
                    }
                    if (event.oldVersion < 4) {
                        if (!database.objectStoreNames.contains(STORE_1.guideTimeline)) {
                            var timeline = database.createObjectStore(STORE_1.guideTimeline, { keyPath: "key" });
                            timeline.createIndex("generationChannelFiniteStart", ["generationKey", "channelKey", "finiteStartMs"], { unique: true });
                            timeline.createIndex("generationChannelOpenStart", ["generationKey", "channelKey", "openStartMs"], { unique: true });
                        }
                        if (!database.objectStoreNames.contains(STORE_1.guideMigration)) {
                            database.createObjectStore(STORE_1.guideMigration, { keyPath: "key" });
                        }
                    }
                };
                openRequest.onblocked = function () {
                    blocked = true;
                    reject(error_1("AIR_IDB_BLOCKED"));
                };
                openRequest.onerror = function () { return reject(error_1(classify_1(openRequest.error))); };
                openRequest.onsuccess = function () {
                    var database = openRequest.result;
                    if (blocked) {
                        database.close();
                        return;
                    }
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
        var guideSourceState_1 = function (key, sourceKey) { return ({
            key: key,
            kind: "source",
            sourceKey: sourceKey,
            epoch: 1,
            mutation: 0,
            activeFeedCount: 0,
            stagedOnlyFeedCount: 0,
            deleted: false,
        }); };
        var guideFeedState_1 = function (key, sourceKey, feedId, sourceEpoch) { return ({
            key: key,
            kind: "feed",
            sourceKey: sourceKey,
            feedId: feedId,
            sourceEpoch: sourceEpoch,
            activeGeneration: null,
            latestGeneration: null,
            nextGeneration: 1,
            revision: 0,
            mutation: 0,
            counts: { channels: 0, programmes: 0 },
            retention: null,
            deleted: false,
        }); };
        var guideSnapshot_1 = function (feed) {
            if (!feed || feed.deleted || feed.activeGeneration === null)
                return null;
            return {
                sourceKey: feed.sourceKey,
                feedId: feed.feedId,
                generation: feed.activeGeneration,
                revision: feed.revision,
                mutationEpoch: feed.mutation,
                counts: feed.counts,
                retention: feed.retention,
            };
        };
        var compareNullable_1 = function (left, right, compare) {
            if (left === null && right === null)
                return 0;
            if (left === null)
                return -1;
            if (right === null)
                return 1;
            return compare(left, right);
        };
        var compareStrings_1 = function (left, right) {
            var count = Math.min(left.length, right.length);
            for (var index = 0; index < count; index += 1) {
                if (left[index] < right[index])
                    return -1;
                if (left[index] > right[index])
                    return 1;
            }
            return left.length - right.length;
        };
        var compareGuideChannels_1 = function (left, right) {
            var comparison = compareStrings_1(left.displayNames, right.displayNames);
            if (comparison !== 0)
                return comparison;
            return compareNullable_1(left.artworkReference, right.artworkReference, function (a, b) { return a < b ? -1 : (a > b ? 1 : 0); });
        };
        var compareGuideProgrammes_1 = function (left, right) {
            var stringCompare = function (a, b) { return a < b ? -1 : (a > b ? 1 : 0); };
            var comparison = compareNullable_1(left.endMs, right.endMs, function (a, b) { return a - b; });
            if (comparison !== 0)
                return comparison;
            comparison = stringCompare(left.title, right.title);
            if (comparison !== 0)
                return comparison;
            comparison = compareNullable_1(left.subtitle, right.subtitle, stringCompare);
            if (comparison !== 0)
                return comparison;
            comparison = compareNullable_1(left.description, right.description, stringCompare);
            if (comparison !== 0)
                return comparison;
            comparison = compareStrings_1(left.categories, right.categories);
            if (comparison !== 0)
                return comparison;
            comparison = compareNullable_1(left.artworkReference, right.artworkReference, stringCompare);
            if (comparison !== 0)
                return comparison;
            return compareNullable_1(left.episode, right.episode, stringCompare);
        };
        var encodedFieldBytes_1 = function (value) { return 16 + (value === null ? 0 : new TextEncoder().encode(value).length); };
        var guideProgrammeBytes_1 = function (programme) {
            return 16 + encodedFieldBytes_1(programme.channelKey) + encodedFieldBytes_1(programme.winnerKey) +
                encodedFieldBytes_1(programme.title) + encodedFieldBytes_1(programme.subtitle) +
                encodedFieldBytes_1(programme.description) + encodedFieldBytes_1(programme.episode) +
                encodedFieldBytes_1(programme.artworkReference) + 4 +
                programme.categories.reduce(function (total, category) { return total + encodedFieldBytes_1(category); }, 0);
        };
        var guideTimelineRow_1 = function (programme) {
            var row = {
                key: programme.key,
                generationKey: programme.generationKey,
                channelKey: programme.channelKey,
                startMs: programme.startMs,
                effectiveEndMs: programme.effectiveEndMs,
            };
            if (programme.endMs === null)
                row.openStartMs = programme.startMs;
            else
                row.finiteStartMs = programme.startMs;
            return row;
        };
        var guideQueuePut_1 = function (tx, generationKey, cleanupAt, kind) {
            if (kind === void 0) { kind = "generation"; }
            tx.objectStore(STORE_1.guideCleanupQueue).put({
                key: "Q|" + generationKey,
                generationKey: generationKey,
                kind: kind,
                cleanupAt: cleanupAt,
            });
        };
        var guideLease_1 = function (tx, command, onValid, setResult, fail) {
            var leases = tx.objectStore(STORE_1.guideLeases);
            if (!command.leaseKey) {
                fail("AIR_IDB_CORRUPT:missing-lease-key");
                return;
            }
            var request;
            try {
                request = leases.get(command.leaseKey);
            }
            catch (failure) {
                fail("AIR_IDB_CORRUPT");
                return;
            }
            request.onerror = function () { return fail(classify_1(request.error)); };
            request.onsuccess = function () {
                var lease = request.result;
                if (!lease || lease.ownerId !== command.ownerId || lease.expiresAt <= command.nowMs) {
                    setResult({ status: "stale" });
                    return;
                }
                if (!lease.generationKey) {
                    fail("AIR_IDB_CORRUPT:missing-lease-generation-key");
                    return;
                }
                var generationRequest;
                try {
                    generationRequest = tx.objectStore(STORE_1.guideGenerations).get(lease.generationKey);
                }
                catch (failure) {
                    fail("AIR_IDB_CORRUPT");
                    return;
                }
                generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                generationRequest.onsuccess = function () {
                    var generation = generationRequest.result;
                    if (!generation || generation.cleanupStarted) {
                        setResult({ status: "stale" });
                        return;
                    }
                    onValid(lease, generation);
                };
            };
        };
        var guideCursorPage_1 = function (store, range, direction, afterKey, limit, mapValue, done, fail) {
            var rows = [];
            var lower = afterKey === null ? range.lower : afterKey;
            var actualRange = IDBKeyRange.bound(lower, range.upper, afterKey !== null, false);
            var request = store.openCursor(actualRange, direction || "next");
            request.onerror = function () { return fail(classify_1(request.error)); };
            request.onsuccess = function () {
                var cursor = request.result;
                if (!cursor) {
                    done(rows, null);
                    return;
                }
                if (rows.length >= limit) {
                    done(rows, rows.length === 0 ? null : rows[rows.length - 1].cursorKey);
                    return;
                }
                var mapped = mapValue(cursor.value, cursor.key);
                if (mapped !== null)
                    rows.push(mapped);
                cursor.continue();
            };
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
                    case "guideMigrateLegacy":
                        return transaction_1(database, [STORE_1.guideGenerations, STORE_1.guideProgrammes, STORE_1.guideTimeline, STORE_1.guideMigration], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var programmes = tx.objectStore(STORE_1.guideProgrammes);
                            var timeline = tx.objectStore(STORE_1.guideTimeline);
                            var migrations = tx.objectStore(STORE_1.guideMigration);
                            var stateRequest = migrations.get("legacy-v3");
                            stateRequest.onerror = function () { return fail(classify_1(stateRequest.error)); };
                            stateRequest.onsuccess = function () {
                                var state = stateRequest.result || {
                                    key: "legacy-v3",
                                    afterKey: null,
                                    pendingGenerationKey: null,
                                    complete: false,
                                };
                                if (state.complete) {
                                    setResult({ status: "ok", migratedRows: 0, hasMore: false });
                                    return;
                                }
                                var migratedRows = 0;
                                var finishGeneration = function (generationKey, done) {
                                    if (generationKey === null) {
                                        done();
                                        return;
                                    }
                                    var request = generations.get(generationKey);
                                    request.onerror = function () { return fail(classify_1(request.error)); };
                                    request.onsuccess = function () {
                                        var generation = request.result;
                                        if (generation) {
                                            generation.timelineMigrated = true;
                                            generations.put(generation);
                                        }
                                        done();
                                    };
                                };
                                var range = state.afterKey === null ? null : IDBKeyRange.lowerBound(state.afterKey, true);
                                var cursorRequest = programmes.openCursor(range);
                                cursorRequest.onerror = function () { return fail(classify_1(cursorRequest.error)); };
                                cursorRequest.onsuccess = function () {
                                    var cursor = cursorRequest.result;
                                    if (!cursor) {
                                        finishGeneration(state.pendingGenerationKey, function () {
                                            state.pendingGenerationKey = null;
                                            state.complete = true;
                                            migrations.put(state);
                                            setResult({ status: "ok", migratedRows: migratedRows, hasMore: false });
                                        });
                                        return;
                                    }
                                    if (migratedRows >= command.maxRows) {
                                        migrations.put(state);
                                        setResult({ status: "ok", migratedRows: migratedRows, hasMore: true });
                                        return;
                                    }
                                    var programme = cursor.value;
                                    var processRow = function () {
                                        state.pendingGenerationKey = programme.generationKey;
                                        var generationRequest = generations.get(programme.generationKey);
                                        generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                                        generationRequest.onsuccess = function () {
                                            var generation = generationRequest.result;
                                            if (generation) {
                                                if (!Number.isSafeInteger(generation.maxFiniteSpanMs))
                                                    generation.maxFiniteSpanMs = 0;
                                                if (!Number.isSafeInteger(generation.minStartMs))
                                                    generation.minStartMs = programme.startMs;
                                                else
                                                    generation.minStartMs = Math.min(generation.minStartMs, programme.startMs);
                                                if (programme.endMs !== null) {
                                                    generation.maxFiniteSpanMs = Math.max(generation.maxFiniteSpanMs, programme.effectiveEndMs - programme.startMs);
                                                }
                                                generations.put(generation);
                                            }
                                            timeline.put(guideTimelineRow_1(programme));
                                            state.afterKey = cursor.primaryKey;
                                            migratedRows += 1;
                                            cursor.continue();
                                        };
                                    };
                                    if (state.pendingGenerationKey !== null &&
                                        state.pendingGenerationKey !== programme.generationKey) {
                                        var previous = state.pendingGenerationKey;
                                        finishGeneration(previous, processRow);
                                    }
                                    else {
                                        processRow();
                                    }
                                };
                            };
                        });
                    case "guideBegin":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var source = null;
                            var feed = null;
                            var ready = 0;
                            var finish = function () {
                                ready += 1;
                                if (ready !== 2)
                                    return;
                                source = source || guideSourceState_1(command.sourceStateKey, command.sourceKey);
                                if (source.deleted) {
                                    source.deleted = false;
                                    source.activeFeedCount = 0;
                                    source.stagedOnlyFeedCount = 0;
                                }
                                feed = feed || guideFeedState_1(command.feedStateKey, command.sourceKey, command.feedId, source.epoch);
                                if (feed.sourceEpoch !== source.epoch) {
                                    feed.sourceEpoch = source.epoch;
                                    feed.activeGeneration = null;
                                    feed.latestGeneration = null;
                                    feed.counts = { channels: 0, programmes: 0 };
                                    feed.retention = null;
                                    feed.deleted = false;
                                    delete feed.activeFeedKey;
                                }
                                var allocate = function () {
                                    var generation = feed.nextGeneration;
                                    if (!Number.isSafeInteger(generation) || generation <= 0) {
                                        fail("AIR_IDB_GENERATION_EXHAUSTED");
                                        return;
                                    }
                                    var component = sortableKey_1(generation);
                                    var generationKey = command.generationPrefix + component;
                                    var wasStagedOnly = feed.activeGeneration === null && feed.latestGeneration !== null;
                                    if (feed.activeGeneration === null && !wasStagedOnly)
                                        source.stagedOnlyFeedCount += 1;
                                    feed.nextGeneration = generation + 1;
                                    feed.latestGeneration = generation;
                                    feed.mutation += 1;
                                    feed.deleted = false;
                                    source.mutation += 1;
                                    var generationRow = {
                                        key: generationKey,
                                        sourceKey: command.sourceKey,
                                        sourceStateKey: command.sourceStateKey,
                                        feedId: command.feedId,
                                        sourceEpoch: source.epoch,
                                        sourceEpochKey: command.sourceKey + "|" + sortableKey_1(source.epoch),
                                        feedStateKey: command.feedStateKey,
                                        generation: generation,
                                        mutationEpoch: feed.mutation,
                                        retention: command.retention,
                                        channelPrefix: command.channelBase + component + "|",
                                        programmePrefix: command.programmeBase + component + "|",
                                        finiteStartPrefix: command.finiteStartBase + component + "|",
                                        openStartPrefix: command.openStartBase + component + "|",
                                        maxFiniteSpanMs: 0,
                                        minStartMs: null,
                                        timelineMigrated: true,
                                        status: "staging",
                                        expiresAt: command.nowMs + command.generationIdleTimeoutMillis,
                                        batchCount: 0,
                                        inputChannelRows: 0,
                                        inputProgrammeRows: 0,
                                        channelCount: 0,
                                        programmeCount: 0,
                                        cleanupStarted: false,
                                    };
                                    states.put(source);
                                    states.put(feed);
                                    tx.objectStore(STORE_1.guideGenerations).put(generationRow);
                                    guideQueuePut_1(tx, generationKey, generationRow.expiresAt);
                                    setResult({ status: "ok", generation: generation, mutationEpoch: feed.mutation, sourceEpoch: source.epoch });
                                };
                                if (feed.latestGeneration === null) {
                                    allocate();
                                }
                                else {
                                    var oldKey = command.generationPrefix + sortableKey_1(feed.latestGeneration);
                                    var oldRequest_1 = tx.objectStore(STORE_1.guideGenerations).get(oldKey);
                                    oldRequest_1.onerror = function () { return fail(classify_1(oldRequest_1.error)); };
                                    oldRequest_1.onsuccess = function () {
                                        var old = oldRequest_1.result;
                                        if (old) {
                                            old.status = "superseded";
                                            old.expiresAt = 0;
                                            tx.objectStore(STORE_1.guideGenerations).put(old);
                                            guideQueuePut_1(tx, old.key, 0);
                                        }
                                        allocate();
                                    };
                                }
                            };
                            var sourceRequest = states.get(command.sourceStateKey);
                            sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                            sourceRequest.onsuccess = function () { source = sourceRequest.result; finish(); };
                            var feedRequest = states.get(command.feedStateKey);
                            feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                            feedRequest.onsuccess = function () { feed = feedRequest.result; finish(); };
                        });
                    case "guideRenewGeneration":
                    case "guideAbandon":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generationRequest = tx.objectStore(STORE_1.guideGenerations).get(command.generationKey);
                            generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                            generationRequest.onsuccess = function () {
                                var generation = generationRequest.result;
                                var abandonablePoison = command.op === "guideAbandon" && generation && generation.status === "poisoned";
                                if (!generation || (generation.status !== "staging" && !abandonablePoison)) {
                                    setResult({ status: "terminal", value: false });
                                    return;
                                }
                                var states = tx.objectStore(STORE_1.guideStates);
                                var sourceRequest = states.get(generation.sourceStateKey);
                                sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                sourceRequest.onsuccess = function () {
                                    var source = sourceRequest.result;
                                    if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                                        setResult({ status: "terminal", value: false });
                                        return;
                                    }
                                    var feedRequest = states.get(generation.feedStateKey);
                                    feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                    feedRequest.onsuccess = function () {
                                        var feed = feedRequest.result;
                                        if (!feed || feed.sourceEpoch !== generation.sourceEpoch ||
                                            feed.latestGeneration !== generation.generation ||
                                            (generation.status === "staging" && generation.expiresAt <= command.nowMs)) {
                                            setResult({ status: "terminal", value: false });
                                            return;
                                        }
                                        if (command.op === "guideRenewGeneration") {
                                            generation.expiresAt = command.nowMs + command.generationIdleTimeoutMillis;
                                            tx.objectStore(STORE_1.guideGenerations).put(generation);
                                            guideQueuePut_1(tx, generation.key, generation.expiresAt);
                                            setResult({ status: "ok", value: true });
                                        }
                                        else {
                                            generation.status = "abandoned";
                                            generation.expiresAt = 0;
                                            feed.latestGeneration = null;
                                            feed.mutation += 1;
                                            if (feed.activeGeneration === null) {
                                                source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                                            }
                                            source.mutation += 1;
                                            states.put(source);
                                            states.put(feed);
                                            tx.objectStore(STORE_1.guideGenerations).put(generation);
                                            guideQueuePut_1(tx, generation.key, 0);
                                            setResult({ status: "ok", value: true });
                                        }
                                    };
                                };
                            };
                        });
                    case "guideRejectStage":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var request = generations.get(command.generationKey);
                            request.onerror = function () { return fail(classify_1(request.error)); };
                            request.onsuccess = function () {
                                var generation = request.result;
                                if (!generation) {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                if (generation.status === "poisoned") {
                                    setResult({ status: "limit" });
                                    return;
                                }
                                if (generation.status !== "staging" || generation.expiresAt <= command.nowMs) {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                var states = tx.objectStore(STORE_1.guideStates);
                                var sourceRequest = states.get(generation.sourceStateKey);
                                sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                sourceRequest.onsuccess = function () {
                                    var source = sourceRequest.result;
                                    if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                                        setResult({ status: "stale" });
                                        return;
                                    }
                                    var feedRequest = states.get(generation.feedStateKey);
                                    feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                    feedRequest.onsuccess = function () {
                                        var feed = feedRequest.result;
                                        if (!feed || feed.sourceEpoch !== generation.sourceEpoch || feed.latestGeneration !== generation.generation) {
                                            setResult({ status: "stale" });
                                            return;
                                        }
                                        generation.batchCount += 1;
                                        generation.inputChannelRows += command.inputChannelRows;
                                        generation.inputProgrammeRows += command.inputProgrammeRows;
                                        generation.status = "poisoned";
                                        generation.expiresAt = 0;
                                        generations.put(generation);
                                        guideQueuePut_1(tx, generation.key, 0);
                                        setResult({ status: "limit" });
                                    };
                                };
                            };
                        });
                    case "guideStage":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideChannels, STORE_1.guideProgrammes, STORE_1.guideTimeline, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var generationRequest = generations.get(command.generationKey);
                            generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                            generationRequest.onsuccess = function () {
                                var generation = generationRequest.result;
                                if (generation && generation.status === "poisoned") {
                                    setResult({ status: "limit" });
                                    return;
                                }
                                if (!generation || generation.status !== "staging" || generation.expiresAt <= command.nowMs) {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                var states = tx.objectStore(STORE_1.guideStates);
                                var sourceRequest = states.get(generation.sourceStateKey);
                                sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                sourceRequest.onsuccess = function () {
                                    var source = sourceRequest.result;
                                    if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                                        setResult({ status: "stale" });
                                        return;
                                    }
                                    var feedRequest = states.get(generation.feedStateKey);
                                    feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                    feedRequest.onsuccess = function () {
                                        var feed = feedRequest.result;
                                        if (!feed || feed.latestGeneration !== generation.generation) {
                                            setResult({ status: "stale" });
                                            return;
                                        }
                                        var batchItems = command.channels.length + command.programmes.length;
                                        generation.batchCount += 1;
                                        generation.inputChannelRows += command.channels.length;
                                        generation.inputProgrammeRows += command.programmes.length;
                                        if (generation.batchCount > command.maxBatches ||
                                            generation.inputChannelRows > command.maxInputChannels ||
                                            generation.inputProgrammeRows > command.maxInputProgrammes ||
                                            batchItems > command.maxBatchItems) {
                                            generation.status = "poisoned";
                                            generation.expiresAt = 0;
                                            generations.put(generation);
                                            guideQueuePut_1(tx, generation.key, 0);
                                            setResult({ status: "limit" });
                                            return;
                                        }
                                        var channelsStore = tx.objectStore(STORE_1.guideChannels);
                                        var programmesStore = tx.objectStore(STORE_1.guideProgrammes);
                                        var channelUpdates = [];
                                        var programmeUpdates = [];
                                        var pending = command.channels.length + command.programmes.length;
                                        var addedChannels = 0;
                                        var addedProgrammes = 0;
                                        var complete = function () {
                                            if (pending !== 0)
                                                return;
                                            var channelCount = generation.channelCount + addedChannels;
                                            var programmeCount = generation.programmeCount + addedProgrammes;
                                            if (channelCount > command.maxChannels || programmeCount > command.maxProgrammes) {
                                                generation.status = "poisoned";
                                                generation.expiresAt = 0;
                                                generations.put(generation);
                                                guideQueuePut_1(tx, generation.key, 0);
                                                setResult({ status: "limit" });
                                                return;
                                            }
                                            channelUpdates.forEach(function (row) { return channelsStore.put(row); });
                                            var timelineStore = tx.objectStore(STORE_1.guideTimeline);
                                            programmeUpdates.forEach(function (row) {
                                                programmesStore.put(row);
                                                timelineStore.put(guideTimelineRow_1(row));
                                            });
                                            generation.channelCount = channelCount;
                                            generation.programmeCount = programmeCount;
                                            generation.expiresAt = command.nowMs + command.generationIdleTimeoutMillis;
                                            generations.put(generation);
                                            guideQueuePut_1(tx, generation.key, generation.expiresAt);
                                            setResult({ status: "ok", counts: { channels: channelCount, programmes: programmeCount } });
                                        };
                                        if (pending === 0)
                                            complete();
                                        command.channels.forEach(function (candidate) {
                                            candidate.sourceKey = generation.sourceKey;
                                            candidate.feedId = generation.feedId;
                                            candidate.generation = generation.generation;
                                            candidate.generationKey = generation.key;
                                            var request = channelsStore.get(candidate.key);
                                            request.onerror = function () { return fail(classify_1(request.error)); };
                                            request.onsuccess = function () {
                                                var current = request.result;
                                                if (!current)
                                                    addedChannels += 1;
                                                if (!current || compareGuideChannels_1(candidate, current) < 0)
                                                    channelUpdates.push(candidate);
                                                pending -= 1;
                                                complete();
                                            };
                                        });
                                        command.programmes.forEach(function (candidate) {
                                            candidate.sourceKey = generation.sourceKey;
                                            candidate.feedId = generation.feedId;
                                            candidate.generation = generation.generation;
                                            candidate.generationKey = generation.key;
                                            generation.minStartMs = generation.minStartMs === null
                                                ? candidate.startMs
                                                : Math.min(generation.minStartMs, candidate.startMs);
                                            if (candidate.endMs === null)
                                                candidate.openStartMs = candidate.startMs;
                                            else {
                                                candidate.finiteStartMs = candidate.startMs;
                                                generation.maxFiniteSpanMs = Math.max(generation.maxFiniteSpanMs, candidate.effectiveEndMs - candidate.startMs);
                                            }
                                            var request = programmesStore.get(candidate.key);
                                            request.onerror = function () { return fail(classify_1(request.error)); };
                                            request.onsuccess = function () {
                                                var current = request.result;
                                                if (!current)
                                                    addedProgrammes += 1;
                                                if (!current || compareGuideProgrammes_1(candidate, current) < 0)
                                                    programmeUpdates.push(candidate);
                                                pending -= 1;
                                                complete();
                                            };
                                        });
                                    };
                                };
                            };
                        });
                    case "guideActivate":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var generationRequest = generations.get(command.generationKey);
                            generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                            generationRequest.onsuccess = function () {
                                var generation = generationRequest.result;
                                if (!generation) {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                if (generation.status === "poisoned") {
                                    setResult({ status: "limit" });
                                    return;
                                }
                                var feedRequest = states.get(generation.feedStateKey);
                                feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                feedRequest.onsuccess = function () {
                                    var feed = feedRequest.result;
                                    if (!feed || feed.latestGeneration !== generation.generation || generation.status !== "staging") {
                                        setResult({ status: "superseded", current: guideSnapshot_1(feed) });
                                        return;
                                    }
                                    if (generation.expiresAt <= command.nowMs) {
                                        setResult({ status: "stale" });
                                        return;
                                    }
                                    if (generation.channelCount !== command.expected.channels ||
                                        generation.programmeCount !== command.expected.programmes) {
                                        setResult({ status: "corrupt" });
                                        return;
                                    }
                                    if (generation.channelCount === 0 && generation.programmeCount === 0) {
                                        setResult({ status: "limit" });
                                        return;
                                    }
                                    var sourceRequest = states.get(command.sourceStateKey);
                                    sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                    sourceRequest.onsuccess = function () {
                                        var source = sourceRequest.result;
                                        if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                                            setResult({ status: "superseded", current: null });
                                            return;
                                        }
                                        var oldGeneration = feed.activeGeneration;
                                        var hadActive = oldGeneration !== null;
                                        feed.activeGeneration = generation.generation;
                                        feed.latestGeneration = null;
                                        feed.revision += 1;
                                        feed.mutation = generation.mutationEpoch;
                                        feed.counts = { channels: generation.channelCount, programmes: generation.programmeCount };
                                        feed.retention = generation.retention;
                                        feed.deleted = false;
                                        feed.activeFeedKey = command.activeFeedBase + sortableKey_1(source.epoch) + "|" + command.feedComponent;
                                        generation.status = "active";
                                        generations.put(generation);
                                        tx.objectStore(STORE_1.guideCleanupQueue).delete("Q|" + generation.key);
                                        if (oldGeneration !== null && oldGeneration !== generation.generation) {
                                            var oldKey = command.generationPrefix + sortableKey_1(oldGeneration);
                                            var oldRequest_2 = generations.get(oldKey);
                                            oldRequest_2.onsuccess = function () {
                                                var old = oldRequest_2.result;
                                                if (old) {
                                                    old.status = "inactive";
                                                    old.expiresAt = 0;
                                                    generations.put(old);
                                                    guideQueuePut_1(tx, old.key, 0);
                                                }
                                            };
                                        }
                                        if (!hadActive) {
                                            source.activeFeedCount += 1;
                                            source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                                        }
                                        source.mutation += 1;
                                        states.put(feed);
                                        states.put(source);
                                        setResult({ status: "published", snapshot: guideSnapshot_1(feed) });
                                    };
                                };
                            };
                        });
                    case "guideSnapshot":
                        return transaction_1(database, [STORE_1.guideStates], "readonly", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var sourceRequest = states.get(command.sourceStateKey);
                            sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                            sourceRequest.onsuccess = function () {
                                var source = sourceRequest.result;
                                if (!source || source.deleted) {
                                    setResult(null);
                                    return;
                                }
                                var feedRequest = states.get(command.feedStateKey);
                                feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                feedRequest.onsuccess = function () {
                                    var feed = feedRequest.result;
                                    setResult(feed && feed.sourceEpoch === source.epoch ? guideSnapshot_1(feed) : null);
                                };
                            };
                        });
                    case "guideSourceSnapshot":
                        return transaction_1(database, [STORE_1.guideStates], "readonly", command.operationId, function (tx, setResult, fail) {
                            var request = tx.objectStore(STORE_1.guideStates).get(command.sourceStateKey);
                            request.onerror = function () { return fail(classify_1(request.error)); };
                            request.onsuccess = function () {
                                var source = request.result || guideSourceState_1(command.sourceStateKey, command.sourceKey);
                                setResult({ status: "ok", sourceKey: command.sourceKey, epoch: source.epoch, mutation: source.mutation, feedCount: source.deleted ? 0 : source.activeFeedCount });
                            };
                        });
                    case "guideSnapshots":
                        return transaction_1(database, [STORE_1.guideStates], "readonly", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var sourceRequest = states.get(command.sourceStateKey);
                            sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                            sourceRequest.onsuccess = function () {
                                var source = sourceRequest.result;
                                if (!source || source.epoch !== command.sourceEpoch || source.mutation !== command.sourceMutation) {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                var prefix = command.activeFeedPrefix;
                                var lower = command.afterKey === null ? prefix : command.afterKey;
                                var range = IDBKeyRange.bound(lower, prefix + "\uffff", command.afterKey !== null, false);
                                var rows = [];
                                var cursorRequest = states.index("activeFeedKey").openCursor(range);
                                cursorRequest.onerror = function () { return fail(classify_1(cursorRequest.error)); };
                                cursorRequest.onsuccess = function () {
                                    var cursor = cursorRequest.result;
                                    if (!cursor || rows.length >= command.limit) {
                                        setResult({ status: "ok", rows: rows, nextKey: cursor ? rows[rows.length - 1].activeFeedKey : null });
                                        return;
                                    }
                                    var feed = cursor.value;
                                    if (feed.sourceEpoch === source.epoch && !feed.deleted)
                                        rows.push(Object.assign(guideSnapshot_1(feed), { activeFeedKey: feed.activeFeedKey }));
                                    cursor.continue();
                                };
                            };
                        });
                    case "guideAcquire":
                        return transaction_1(database, [STORE_1.guideGenerations, STORE_1.guideLeases], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var generationRequest = generations.get(command.generationKey);
                            generationRequest.onerror = function () { return fail(classify_1(generationRequest.error)); };
                            generationRequest.onsuccess = function () {
                                var generation = generationRequest.result;
                                if (!generation || generation.cleanupStarted || generation.generation !== command.generation) {
                                    setResult({ status: "missing" });
                                    return;
                                }
                                var leases = tx.objectStore(STORE_1.guideLeases);
                                var expiry = leases.index("expiresAt");
                                var expiredRequest = expiry.openCursor(IDBKeyRange.upperBound(command.nowMs));
                                var liveCount = 0;
                                expiredRequest.onerror = function () { return fail(classify_1(expiredRequest.error)); };
                                expiredRequest.onsuccess = function () {
                                    var cursor = expiredRequest.result;
                                    if (cursor) {
                                        cursor.delete();
                                        cursor.continue();
                                        return;
                                    }
                                    var countRequest = leases.count();
                                    countRequest.onerror = function () { return fail(classify_1(countRequest.error)); };
                                    countRequest.onsuccess = function () {
                                        liveCount = countRequest.result;
                                        if (liveCount >= command.maxLiveLeases) {
                                            setResult({ status: "limit" });
                                            return;
                                        }
                                        leases.put({
                                            key: command.leaseKey,
                                            ownerId: command.ownerId,
                                            generationKey: command.generationKey,
                                            expiresAt: command.nowMs + command.leaseIdleTimeoutMillis,
                                        });
                                        setResult({ status: "ok" });
                                    };
                                };
                            };
                        });
                    case "guideRenewLease":
                    case "guideReleaseLease":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var leases = tx.objectStore(STORE_1.guideLeases);
                            var request = leases.get(command.leaseKey);
                            request.onerror = function () { return fail(classify_1(request.error)); };
                            request.onsuccess = function () {
                                var lease = request.result;
                                if (!lease || lease.ownerId !== command.ownerId) {
                                    setResult({ status: "ok", value: false });
                                    return;
                                }
                                if (command.op === "guideReleaseLease") {
                                    leases.delete(lease.key);
                                    var queue_1 = tx.objectStore(STORE_1.guideCleanupQueue);
                                    var queueRequest_1 = queue_1.get("Q|" + lease.generationKey);
                                    queueRequest_1.onerror = function () { return fail(classify_1(queueRequest_1.error)); };
                                    queueRequest_1.onsuccess = function () {
                                        var queueRow = queueRequest_1.result;
                                        if (queueRow) {
                                            queueRow.cleanupAt = 0;
                                            queue_1.put(queueRow);
                                        }
                                        setResult({ status: "ok", value: true });
                                    };
                                }
                                else if (lease.expiresAt <= command.nowMs) {
                                    leases.delete(lease.key);
                                    setResult({ status: "ok", value: false });
                                }
                                else {
                                    lease.expiresAt = command.nowMs + command.leaseIdleTimeoutMillis;
                                    leases.put(lease);
                                    setResult({ status: "ok", value: true });
                                }
                            };
                        });
                    case "guideChannels":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideGenerations, STORE_1.guideChannels], "readonly", command.operationId, function (tx, setResult, fail) {
                            guideLease_1(tx, command, function (_lease, generation) {
                                var prefix = command.channelPrefix;
                                guideCursorPage_1(tx.objectStore(STORE_1.guideChannels), { lower: prefix, upper: prefix + "\uffff" }, "next", command.afterKey, command.limit, function (row, key) { return Object.assign({ cursorKey: key }, row); }, function (rows, nextKey) { return setResult({ status: "ok", rows: rows, nextKey: nextKey }); }, fail);
                            }, setResult, fail);
                        });
                    case "guideSearchRows":
                    case "guideFullProgrammes":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideGenerations, STORE_1.guideProgrammes], "readonly", command.operationId, function (tx, setResult, fail) {
                            guideLease_1(tx, command, function (_lease, generation) {
                                var prefix = command.programmePrefix;
                                guideCursorPage_1(tx.objectStore(STORE_1.guideProgrammes), { lower: prefix, upper: prefix + "\uffff" }, "next", command.afterKey, command.limit, function (row, key) { return command.op === "guideSearchRows"
                                    ? {
                                        cursorKey: key,
                                        locatorKey: key,
                                        startMs: row.startMs,
                                        effectiveEndMs: row.effectiveEndMs,
                                        title: row.title,
                                        subtitle: row.subtitle,
                                    }
                                    : Object.assign({ cursorKey: key, locatorKey: key }, row); }, function (rows, nextKey) { return setResult({ status: "ok", rows: rows, nextKey: nextKey }); }, fail);
                            }, setResult, fail);
                        });
                    case "guideProgramme":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideGenerations, STORE_1.guideProgrammes], "readonly", command.operationId, function (tx, setResult, fail) {
                            guideLease_1(tx, command, function (_lease, generation) {
                                if (command.locatorGenerationKey !== generation.key || !command.locatorKey.startsWith(command.programmePrefix)) {
                                    setResult({ status: "ok", row: null });
                                    return;
                                }
                                var request = tx.objectStore(STORE_1.guideProgrammes).get(command.locatorKey);
                                request.onerror = function () { return fail(classify_1(request.error)); };
                                request.onsuccess = function () { return setResult({ status: "ok", row: request.result || null }); };
                            }, setResult, fail);
                        });
                    case "durableGuideWindow":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideGenerations, STORE_1.guideProgrammes, STORE_1.guideTimeline, STORE_1.guideMigration], "readonly", command.operationId, function (tx, setResult, fail) {
                            guideLease_1(tx, command, function (_lease, generation) {
                                var store = tx.objectStore(STORE_1.guideProgrammes);
                                var rows = [];
                                var payloadBytes = 0;
                                var visits = 0;
                                var finish = function (truncated) { return setResult({
                                    status: "ok",
                                    rows: rows,
                                    nextStartMs: truncated && rows.length > 0 ? rows[rows.length - 1].startMs : null,
                                    truncated: truncated,
                                    payloadBytes: payloadBytes,
                                }); };
                                var accept = function (row, locatorKey, advance) {
                                    if (row.effectiveEndMs <= command.fromMs) {
                                        advance();
                                        return;
                                    }
                                    var bytes = guideProgrammeBytes_1(row);
                                    if (rows.length >= command.limit || payloadBytes + bytes > command.payloadByteLimit) {
                                        finish(true);
                                        return;
                                    }
                                    rows.push(Object.assign({ locatorKey: locatorKey }, row));
                                    payloadBytes += bytes;
                                    advance();
                                };
                                var runLegacy = function () {
                                    var index = store.index("generationChannelStart");
                                    var lower = command.afterStartMs === null ? Number.MIN_SAFE_INTEGER : command.afterStartMs;
                                    var range = IDBKeyRange.bound([generation.key, command.channelKey, lower], [generation.key, command.channelKey, command.untilMs], command.afterStartMs !== null, true);
                                    var request = index.openCursor(range);
                                    request.onerror = function () { return fail(classify_1(request.error)); };
                                    request.onsuccess = function () {
                                        var cursor = request.result;
                                        if (!cursor) {
                                            finish(false);
                                            return;
                                        }
                                        if (visits >= command.maxIndexVisits) {
                                            setResult({ status: "limit" });
                                            return;
                                        }
                                        visits += 1;
                                        accept(cursor.value, cursor.primaryKey, function () { return cursor.continue(); });
                                    };
                                };
                                var runTimeline = function () {
                                    if (generation.minStartMs === null) {
                                        finish(false);
                                        return;
                                    }
                                    var timeline = tx.objectStore(STORE_1.guideTimeline);
                                    var finiteIndex = timeline.index("generationChannelFiniteStart");
                                    var openIndex = timeline.index("generationChannelOpenStart");
                                    var finiteFloor = Math.max(Number.MIN_SAFE_INTEGER, command.fromMs - generation.maxFiniteSpanMs);
                                    var finiteLower = command.afterStartMs === null
                                        ? finiteFloor
                                        : Math.max(finiteFloor, command.afterStartMs);
                                    var openLower = command.afterStartMs === null
                                        ? generation.minStartMs
                                        : Math.max(generation.minStartMs, command.afterStartMs);
                                    var finiteRange = IDBKeyRange.bound([generation.key, command.channelKey, finiteLower], [generation.key, command.channelKey, command.untilMs], command.afterStartMs !== null && command.afterStartMs >= finiteFloor, true);
                                    var openRange = IDBKeyRange.bound([generation.key, command.channelKey, openLower], [generation.key, command.channelKey, command.untilMs], command.afterStartMs !== null && command.afterStartMs >= generation.minStartMs, true);
                                    var finiteCursor;
                                    var openCursor;
                                    var finiteReady = false;
                                    var openReady = false;
                                    var advance = function (kind, cursor) {
                                        if (kind === "finite")
                                            finiteReady = false;
                                        else
                                            openReady = false;
                                        cursor.continue();
                                    };
                                    var pump = function () {
                                        if (!finiteReady || !openReady)
                                            return;
                                        if (!finiteCursor && !openCursor) {
                                            finish(false);
                                            return;
                                        }
                                        var kind;
                                        var cursor;
                                        if (!openCursor || (finiteCursor && (finiteCursor.value.startMs < openCursor.value.startMs ||
                                            (finiteCursor.value.startMs === openCursor.value.startMs && finiteCursor.primaryKey < openCursor.primaryKey)))) {
                                            kind = "finite";
                                            cursor = finiteCursor;
                                        }
                                        else {
                                            kind = "open";
                                            cursor = openCursor;
                                        }
                                        if (visits >= command.maxIndexVisits) {
                                            setResult({ status: "limit" });
                                            return;
                                        }
                                        visits += 1;
                                        var fullRequest = store.get(cursor.primaryKey);
                                        fullRequest.onerror = function () { return fail(classify_1(fullRequest.error)); };
                                        fullRequest.onsuccess = function () {
                                            var row = fullRequest.result;
                                            if (!row) {
                                                advance(kind, cursor);
                                                return;
                                            }
                                            accept(row, cursor.primaryKey, function () { return advance(kind, cursor); });
                                        };
                                    };
                                    var finiteRequest = finiteIndex.openCursor(finiteRange);
                                    finiteRequest.onerror = function () { return fail(classify_1(finiteRequest.error)); };
                                    finiteRequest.onsuccess = function () { finiteCursor = finiteRequest.result; finiteReady = true; pump(); };
                                    var openRequest = openIndex.openCursor(openRange);
                                    openRequest.onerror = function () { return fail(classify_1(openRequest.error)); };
                                    openRequest.onsuccess = function () { openCursor = openRequest.result; openReady = true; pump(); };
                                };
                                var migrationRequest = tx.objectStore(STORE_1.guideMigration).get("legacy-v3");
                                migrationRequest.onerror = function () { return fail(classify_1(migrationRequest.error)); };
                                migrationRequest.onsuccess = function () {
                                    var migration = migrationRequest.result;
                                    if (generation.timelineMigrated === true || (migration && migration.complete))
                                        runTimeline();
                                    else
                                        runLegacy();
                                };
                            }, setResult, fail);
                        });
                    case "guideNowNext":
                        return transaction_1(database, [STORE_1.guideLeases, STORE_1.guideGenerations, STORE_1.guideProgrammes], "readonly", command.operationId, function (tx, setResult, fail) {
                            guideLease_1(tx, command, function (_lease, generation) {
                                var store = tx.objectStore(STORE_1.guideProgrammes);
                                var prefix = command.channelProgrammePrefix;
                                var current = undefined;
                                var next = undefined;
                                var complete = function () {
                                    if (current !== undefined && next !== undefined)
                                        setResult({ status: "ok", current: current, next: next });
                                };
                                var currentRequest = store.openCursor(IDBKeyRange.bound(prefix, prefix + command.atKey + "|\uffff"), "prev");
                                currentRequest.onerror = function () { return fail(classify_1(currentRequest.error)); };
                                currentRequest.onsuccess = function () {
                                    var cursor = currentRequest.result;
                                    if (!cursor) {
                                        current = null;
                                        complete();
                                        return;
                                    }
                                    var row = cursor.value;
                                    if (row.effectiveEndMs > command.atMs) {
                                        current = row;
                                        complete();
                                    }
                                    else
                                        cursor.continue();
                                };
                                var nextRequest = store.openCursor(IDBKeyRange.bound(prefix + command.atKey + "|\uffff", prefix + "\uffff", true, false), "next");
                                nextRequest.onerror = function () { return fail(classify_1(nextRequest.error)); };
                                nextRequest.onsuccess = function () {
                                    var cursor = nextRequest.result;
                                    next = cursor ? cursor.value : null;
                                    complete();
                                };
                            }, setResult, fail);
                        });
                    case "guideBeginPrune":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var feedRequest = states.get(command.feedStateKey);
                            feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                            feedRequest.onsuccess = function () {
                                var feed = feedRequest.result;
                                if (!feed || feed.deleted || feed.activeGeneration === null ||
                                    feed.revision !== command.expectedRevision || feed.mutation !== command.expectedMutationEpoch) {
                                    setResult({ status: "superseded", current: guideSnapshot_1(feed) });
                                    return;
                                }
                                if (command.retention.anchorMs !== feed.retention.anchorMs ||
                                    command.retention.retainedFromMs < feed.retention.retainedFromMs ||
                                    command.retention.retainedUntilMs > feed.retention.retainedUntilMs) {
                                    setResult({ status: "limit" });
                                    return;
                                }
                                var sourceRequest = states.get(command.sourceStateKey);
                                sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                sourceRequest.onsuccess = function () {
                                    var source = sourceRequest.result;
                                    if (!source || source.deleted || source.epoch !== feed.sourceEpoch) {
                                        setResult({ status: "superseded", current: null });
                                        return;
                                    }
                                    var generations = tx.objectStore(STORE_1.guideGenerations);
                                    var allocate = function () {
                                        var generation = feed.nextGeneration;
                                        if (!Number.isSafeInteger(generation) || generation <= 0) {
                                            fail("AIR_IDB_GENERATION_EXHAUSTED");
                                            return;
                                        }
                                        var component = sortableKey_1(generation);
                                        var generationKey = command.generationPrefix + component;
                                        feed.nextGeneration = generation + 1;
                                        feed.latestGeneration = generation;
                                        feed.mutation += 1;
                                        source.mutation += 1;
                                        var row = {
                                            key: generationKey,
                                            sourceKey: command.sourceKey,
                                            sourceStateKey: command.sourceStateKey,
                                            feedId: command.feedId,
                                            sourceEpoch: source.epoch,
                                            sourceEpochKey: command.sourceKey + "|" + sortableKey_1(source.epoch),
                                            feedStateKey: command.feedStateKey,
                                            generation: generation,
                                            mutationEpoch: feed.mutation,
                                            retention: command.retention,
                                            channelPrefix: command.channelBase + component + "|",
                                            programmePrefix: command.programmeBase + component + "|",
                                            finiteStartPrefix: command.finiteStartBase + component + "|",
                                            openStartPrefix: command.openStartBase + component + "|",
                                            maxFiniteSpanMs: 0,
                                            minStartMs: null,
                                            timelineMigrated: true,
                                            status: "staging",
                                            purpose: "prune",
                                            expiresAt: command.nowMs + command.generationIdleTimeoutMillis,
                                            batchCount: 0,
                                            inputChannelRows: 0,
                                            inputProgrammeRows: 0,
                                            channelCount: 0,
                                            programmeCount: 0,
                                            cleanupStarted: false,
                                        };
                                        states.put(feed);
                                        states.put(source);
                                        generations.put(row);
                                        guideQueuePut_1(tx, row.key, row.expiresAt);
                                        setResult({ status: "ok", generation: generation, mutationEpoch: feed.mutation, sourceEpoch: source.epoch });
                                    };
                                    if (feed.latestGeneration === null) {
                                        allocate();
                                        return;
                                    }
                                    var oldKey = command.generationPrefix + sortableKey_1(feed.latestGeneration);
                                    var oldRequest = generations.get(oldKey);
                                    oldRequest.onerror = function () { return fail(classify_1(oldRequest.error)); };
                                    oldRequest.onsuccess = function () {
                                        var old = oldRequest.result;
                                        if (old) {
                                            old.status = "superseded";
                                            old.expiresAt = 0;
                                            generations.put(old);
                                            guideQueuePut_1(tx, old.key, 0);
                                        }
                                        allocate();
                                    };
                                };
                            };
                        });
                    case "guideFinishPruneUnchanged":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var generations = tx.objectStore(STORE_1.guideGenerations);
                            var request = generations.get(command.generationKey);
                            request.onerror = function () { return fail(classify_1(request.error)); };
                            request.onsuccess = function () {
                                var generation = request.result;
                                if (!generation || generation.status !== "staging") {
                                    setResult({ status: "stale" });
                                    return;
                                }
                                var states = tx.objectStore(STORE_1.guideStates);
                                var feedRequest = states.get(generation.feedStateKey);
                                feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                feedRequest.onsuccess = function () {
                                    var feed = feedRequest.result;
                                    if (!feed || feed.latestGeneration !== generation.generation) {
                                        setResult({ status: "superseded", current: guideSnapshot_1(feed) });
                                        return;
                                    }
                                    var sourceRequest = states.get(generation.sourceStateKey);
                                    sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                                    sourceRequest.onsuccess = function () {
                                        var source = sourceRequest.result;
                                        if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                                            setResult({ status: "superseded", current: null });
                                            return;
                                        }
                                        generation.status = "abandoned";
                                        generation.expiresAt = 0;
                                        feed.latestGeneration = null;
                                        generations.put(generation);
                                        states.put(feed);
                                        guideQueuePut_1(tx, generation.key, 0);
                                        setResult({ status: "unchanged", current: guideSnapshot_1(feed) });
                                    };
                                };
                            };
                        });
                    case "guideDelete":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideGenerations, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var sourceRequest = states.get(command.sourceStateKey);
                            sourceRequest.onerror = function () { return fail(classify_1(sourceRequest.error)); };
                            sourceRequest.onsuccess = function () {
                                var source = sourceRequest.result || guideSourceState_1(command.sourceStateKey, command.sourceKey);
                                var feedRequest = states.get(command.feedStateKey);
                                feedRequest.onerror = function () { return fail(classify_1(feedRequest.error)); };
                                feedRequest.onsuccess = function () {
                                    var feed = feedRequest.result || guideFeedState_1(command.feedStateKey, command.sourceKey, command.feedId, source.epoch);
                                    if (command.conditional && (feed.activeGeneration === null || feed.revision !== command.expectedRevision ||
                                        feed.mutation !== command.expectedMutationEpoch)) {
                                        setResult({ status: "superseded", current: guideSnapshot_1(feed) });
                                        return;
                                    }
                                    var generations = tx.objectStore(STORE_1.guideGenerations);
                                    var mark = function (generation, status) {
                                        if (generation === null)
                                            return;
                                        var key = command.generationPrefix + sortableKey_1(generation);
                                        var request = generations.get(key);
                                        request.onsuccess = function () {
                                            var row = request.result;
                                            if (row) {
                                                row.status = status;
                                                row.expiresAt = 0;
                                                generations.put(row);
                                                guideQueuePut_1(tx, row.key, 0);
                                            }
                                        };
                                    };
                                    var hadActive = feed.activeGeneration !== null;
                                    var hadStagedOnly = !hadActive && feed.latestGeneration !== null;
                                    mark(feed.activeGeneration, "inactive");
                                    mark(feed.latestGeneration, "abandoned");
                                    feed.activeGeneration = null;
                                    feed.latestGeneration = null;
                                    feed.revision += 1;
                                    feed.mutation += 1;
                                    feed.deleted = true;
                                    feed.counts = { channels: 0, programmes: 0 };
                                    feed.retention = null;
                                    delete feed.activeFeedKey;
                                    if (hadActive)
                                        source.activeFeedCount = Math.max(0, source.activeFeedCount - 1);
                                    if (hadStagedOnly)
                                        source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                                    source.mutation += 1;
                                    states.put(feed);
                                    states.put(source);
                                    setResult({ status: "deleted", revision: feed.revision });
                                };
                            };
                        });
                    case "guideDeleteSource":
                        return transaction_1(database, [STORE_1.guideStates, STORE_1.guideCleanupQueue], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var states = tx.objectStore(STORE_1.guideStates);
                            var request = states.get(command.sourceStateKey);
                            request.onerror = function () { return fail(classify_1(request.error)); };
                            request.onsuccess = function () {
                                var source = request.result || guideSourceState_1(command.sourceStateKey, command.sourceKey);
                                if (command.conditional && (source.epoch !== command.sourceEpoch || source.mutation !== command.sourceMutation)) {
                                    setResult({ status: "superseded", activeFeedCount: source.deleted ? 0 : source.activeFeedCount, stagedOnlyFeedCount: source.deleted ? 0 : source.stagedOnlyFeedCount });
                                    return;
                                }
                                var active = source.deleted ? 0 : source.activeFeedCount;
                                var staged = source.deleted ? 0 : source.stagedOnlyFeedCount;
                                var oldEpoch = source.epoch;
                                source.epoch += 1;
                                source.mutation += 1;
                                source.activeFeedCount = 0;
                                source.stagedOnlyFeedCount = 0;
                                source.deleted = true;
                                states.put(source);
                                tx.objectStore(STORE_1.guideCleanupQueue).put({
                                    key: "QS|" + command.sourceKey + "|" + sortableKey_1(oldEpoch),
                                    kind: "source",
                                    sourceKey: command.sourceKey,
                                    sourceEpoch: oldEpoch,
                                    cleanupAt: 0,
                                });
                                setResult({ status: "deleted", activeFeedCount: active, stagedOnlyFeedCount: staged });
                            };
                        });
                    case "guideCleanup":
                        return transaction_1(database, [
                            STORE_1.guideStates,
                            STORE_1.guideGenerations,
                            STORE_1.guideChannels,
                            STORE_1.guideProgrammes,
                            STORE_1.guideTimeline,
                            STORE_1.guideLeases,
                            STORE_1.guideCleanupQueue,
                        ], "readwrite", command.operationId, function (tx, setResult, fail) {
                            var queue = tx.objectStore(STORE_1.guideCleanupQueue);
                            var queueIndex = queue.index("cleanupAt");
                            var finish = function (removedRows) {
                                var moreRequest = queueIndex.openKeyCursor(IDBKeyRange.upperBound(command.nowMs));
                                moreRequest.onerror = function () { return fail(classify_1(moreRequest.error)); };
                                moreRequest.onsuccess = function () { return setResult({
                                    status: "ok",
                                    removedRows: removedRows,
                                    hasMore: moreRequest.result !== null,
                                }); };
                            };
                            var processGeneration = function (generation, queueRow) {
                                if (!generation) {
                                    queue.delete(queueRow.key);
                                    finish(0);
                                    return;
                                }
                                if (generation.status === "staging" && generation.expiresAt > command.nowMs) {
                                    queueRow.cleanupAt = generation.expiresAt;
                                    queue.put(queueRow);
                                    finish(0);
                                    return;
                                }
                                var leases = tx.objectStore(STORE_1.guideLeases);
                                var leaseRequest = leases.index("generationKey").openCursor(IDBKeyRange.only(generation.key));
                                leaseRequest.onerror = function () { return fail(classify_1(leaseRequest.error)); };
                                leaseRequest.onsuccess = function () {
                                    var cursor = leaseRequest.result;
                                    if (cursor) {
                                        var lease = cursor.value;
                                        if (lease.expiresAt <= command.nowMs) {
                                            cursor.delete();
                                            queueRow.cleanupAt = 0;
                                        }
                                        else {
                                            queueRow.cleanupAt = lease.expiresAt;
                                        }
                                        queue.put(queueRow);
                                        finish(0);
                                        return;
                                    }
                                    generation.cleanupStarted = true;
                                    generation.status = generation.status === "staging" ? "expired" : generation.status;
                                    tx.objectStore(STORE_1.guideGenerations).put(generation);
                                    var remaining = command.maxRows;
                                    var removed = 0;
                                    var tasks = [
                                        [STORE_1.guideChannels, generation.channelPrefix, false],
                                        [STORE_1.guideProgrammes, generation.programmePrefix, true],
                                    ];
                                    var taskIndex = 0;
                                    var runTask = function () {
                                        if (remaining === 0 || taskIndex >= tasks.length) {
                                            checkEmpty();
                                            return;
                                        }
                                        var task = tasks[taskIndex++];
                                        var request = tx.objectStore(task[0]).openCursor(rangeForPrefix_1(task[1]));
                                        request.onerror = function () { return fail(classify_1(request.error)); };
                                        request.onsuccess = function () {
                                            var cursor = request.result;
                                            if (!cursor || remaining === 0) {
                                                runTask();
                                                return;
                                            }
                                            cursor.delete();
                                            if (task[2])
                                                tx.objectStore(STORE_1.guideTimeline).delete(cursor.primaryKey);
                                            remaining -= 1;
                                            removed += 1;
                                            cursor.continue();
                                        };
                                    };
                                    var checkEmpty = function () {
                                        var channelsCount = tx.objectStore(STORE_1.guideChannels)
                                            .openKeyCursor(rangeForPrefix_1(generation.channelPrefix));
                                        var programmesCount = tx.objectStore(STORE_1.guideProgrammes)
                                            .openKeyCursor(rangeForPrefix_1(generation.programmePrefix));
                                        var pending = 2;
                                        var hasPayload = false;
                                        var done = function (request) {
                                            hasPayload = hasPayload || request.result !== null;
                                            pending -= 1;
                                            if (pending !== 0)
                                                return;
                                            if (!hasPayload) {
                                                tx.objectStore(STORE_1.guideGenerations).delete(generation.key);
                                                if (queueRow.kind === "source") {
                                                    queueRow.cleanupAt = 0;
                                                    queue.put(queueRow);
                                                }
                                                else {
                                                    queue.delete(queueRow.key);
                                                }
                                                var states_1 = tx.objectStore(STORE_1.guideStates);
                                                var feedRequest_1 = states_1.get(generation.feedStateKey);
                                                feedRequest_1.onerror = function () { return fail(classify_1(feedRequest_1.error)); };
                                                feedRequest_1.onsuccess = function () {
                                                    var feed = feedRequest_1.result;
                                                    if (feed && feed.sourceEpoch === generation.sourceEpoch && feed.latestGeneration === generation.generation) {
                                                        feed.latestGeneration = null;
                                                        feed.mutation += 1;
                                                        states_1.put(feed);
                                                        var sourceRequest_1 = states_1.get("GS|" + generation.sourceKey);
                                                        sourceRequest_1.onerror = function () { return fail(classify_1(sourceRequest_1.error)); };
                                                        sourceRequest_1.onsuccess = function () {
                                                            var source = sourceRequest_1.result;
                                                            if (source && source.epoch === generation.sourceEpoch && !source.deleted) {
                                                                if (feed.activeGeneration === null)
                                                                    source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                                                                source.mutation += 1;
                                                                states_1.put(source);
                                                            }
                                                        };
                                                    }
                                                };
                                            }
                                            else {
                                                queueRow.cleanupAt = 0;
                                                queue.put(queueRow);
                                            }
                                            finish(removed);
                                        };
                                        channelsCount.onerror = function () { return fail(classify_1(channelsCount.error)); };
                                        programmesCount.onerror = function () { return fail(classify_1(programmesCount.error)); };
                                        channelsCount.onsuccess = function () { return done(channelsCount); };
                                        programmesCount.onsuccess = function () { return done(programmesCount); };
                                    };
                                    runTask();
                                };
                            };
                            var queueRequest = queueIndex.openCursor(IDBKeyRange.upperBound(command.nowMs));
                            queueRequest.onerror = function () { return fail(classify_1(queueRequest.error)); };
                            queueRequest.onsuccess = function () {
                                var cursor = queueRequest.result;
                                if (!cursor) {
                                    setResult({ status: "ok", removedRows: 0, hasMore: false });
                                    return;
                                }
                                var queueRow = cursor.value;
                                if (queueRow.kind === "source") {
                                    var sourceEpochKey = queueRow.sourceKey + "|" + sortableKey_1(queueRow.sourceEpoch);
                                    var generationRequest_1 = tx.objectStore(STORE_1.guideGenerations)
                                        .index("sourceEpochKey").openCursor(IDBKeyRange.only(sourceEpochKey));
                                    generationRequest_1.onerror = function () { return fail(classify_1(generationRequest_1.error)); };
                                    generationRequest_1.onsuccess = function () {
                                        var generationCursor = generationRequest_1.result;
                                        if (!generationCursor) {
                                            queue.delete(queueRow.key);
                                            finish(0);
                                            return;
                                        }
                                        var generation = generationCursor.value;
                                        processGeneration(generation, {
                                            key: queueRow.key,
                                            kind: "source",
                                            sourceKey: queueRow.sourceKey,
                                            sourceEpoch: queueRow.sourceEpoch,
                                            generationKey: generation.key,
                                            cleanupAt: 0,
                                        });
                                    };
                                }
                                else {
                                    var generationRequest_2 = tx.objectStore(STORE_1.guideGenerations).get(queueRow.generationKey);
                                    generationRequest_2.onerror = function () { return fail(classify_1(generationRequest_2.error)); };
                                    generationRequest_2.onsuccess = function () { return processGeneration(generationRequest_2.result, queueRow); };
                                }
                            };
                        });
                    case "guideDebugDump":
                        return transaction_1(database, [
                            STORE_1.guideStates,
                            STORE_1.guideGenerations,
                            STORE_1.guideChannels,
                            STORE_1.guideProgrammes,
                            STORE_1.guideTimeline,
                            STORE_1.guideMigration,
                            STORE_1.guideLeases,
                            STORE_1.guideCleanupQueue,
                        ], "readonly", command.operationId, function (tx, setResult, fail) {
                            var stores = [
                                STORE_1.guideStates,
                                STORE_1.guideGenerations,
                                STORE_1.guideChannels,
                                STORE_1.guideProgrammes,
                                STORE_1.guideTimeline,
                                STORE_1.guideMigration,
                                STORE_1.guideLeases,
                                STORE_1.guideCleanupQueue,
                            ];
                            var records = [];
                            var index = 0;
                            var nextStore = function () {
                                if (index >= stores.length || records.length >= command.limit) {
                                    setResult({ status: "ok", records: records });
                                    return;
                                }
                                var request = tx.objectStore(stores[index++]).openCursor();
                                request.onerror = function () { return fail(classify_1(request.error)); };
                                request.onsuccess = function () {
                                    var cursor = request.result;
                                    if (!cursor) {
                                        nextStore();
                                        return;
                                    }
                                    records.push(stringify_1(cursor.value));
                                    if (records.length >= command.limit)
                                        setResult({ status: "ok", records: records });
                                    else
                                        cursor.continue();
                                };
                            };
                            nextStore();
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
