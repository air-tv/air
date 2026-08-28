(databaseName, commandJson) => {
  const runtimeKey = "__airCatalogIndexedDbRuntimeV1";
  const root = globalThis;
  if (!root[runtimeKey]) {
    const STORE = Object.freeze({
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
    const ALL_STORES = Object.freeze(Object.values(STORE));
    const MEDIA_STORES = Object.freeze([
      STORE.sources,
      STORE.generations,
      STORE.orphanQueue,
      STORE.catalogRecords,
      STORE.channelRecords,
      STORE.programmes,
      STORE.counters,
    ]);
    const connections = new Map();
    const transactions = new Map();

    const error = (code) => {
      const failure = new Error(code);
      failure.name = "AirIndexedDbError";
      return failure;
    };
    const classify = (failure) => {
      const name = failure && failure.name ? failure.name : "";
      if (name === "QuotaExceededError") return "AIR_IDB_QUOTA";
      if (name === "AbortError") return "AIR_IDB_ABORT";
      if (name === "InvalidStateError" || name === "SecurityError") return "AIR_IDB_UNAVAILABLE";
      return "AIR_IDB_FAILURE";
    };
    const parse = (value) => value === undefined ? null : (typeof value === "string" ? JSON.parse(value) : value);
    const stringify = (value) => JSON.stringify(value);
    const sortableKey = (value) => String(value).padStart(16, "0");
    const rangeForPrefix = (prefix) => IDBKeyRange.bound(prefix, prefix + "\uffff");
    const request = (operation) => new Promise((resolve, reject) => {
      operation.onsuccess = () => resolve(operation.result);
      operation.onerror = () => reject(error(classify(operation.error)));
    });
    const open = (name) => {
      const existing = connections.get(name);
      if (existing) return Promise.resolve(existing);
      if (!root.indexedDB) return Promise.reject(error("AIR_IDB_UNAVAILABLE"));
      return new Promise((resolve, reject) => {
        let openRequest;
        let blocked = false;
        try {
          openRequest = root.indexedDB.open(name, 4);
        } catch (failure) {
          reject(error(classify(failure)));
          return;
        }
        openRequest.onupgradeneeded = (event) => {
          const database = openRequest.result;
          if (event.oldVersion < 2) {
            for (const store of Array.from(database.objectStoreNames)) database.deleteObjectStore(store);
            for (const store of MEDIA_STORES) {
              if (store !== STORE.catalogRecords && store !== STORE.channelRecords) database.createObjectStore(store);
            }
            database.createObjectStore(STORE.catalogRecords, { keyPath: "recordKey" })
              .createIndex("orderKey", "orderKey", { unique: true });
            database.createObjectStore(STORE.channelRecords, { keyPath: "recordKey" })
              .createIndex("orderKey", "orderKey", { unique: true });
          }
          if (event.oldVersion < 3) {
            if (!database.objectStoreNames.contains(STORE.guideStates)) {
              database.createObjectStore(STORE.guideStates, { keyPath: "key" })
                .createIndex("activeFeedKey", "activeFeedKey", { unique: true });
            }
            if (!database.objectStoreNames.contains(STORE.guideGenerations)) {
              const generations = database.createObjectStore(STORE.guideGenerations, { keyPath: "key" });
              generations.createIndex("sourceEpochKey", "sourceEpochKey", { unique: false });
              generations.createIndex("sourceFeedGeneration", ["sourceKey", "feedId", "generation"], { unique: true });
            }
            if (!database.objectStoreNames.contains(STORE.guideChannels)) {
              database.createObjectStore(STORE.guideChannels, { keyPath: "key" })
                .createIndex("generationChannel", ["generationKey", "channelKey"], { unique: true });
            }
            if (!database.objectStoreNames.contains(STORE.guideProgrammes)) {
              const programmes = database.createObjectStore(STORE.guideProgrammes, { keyPath: "key" });
              programmes.createIndex("endKey", "endKey", { unique: true });
              programmes.createIndex(
                "generationChannelStart",
                ["generationKey", "channelKey", "startMs"],
                { unique: true },
              );
              programmes.createIndex(
                "generationChannelEffectiveEnd",
                ["generationKey", "channelKey", "effectiveEndMs", "startMs"],
                { unique: true },
              );
              programmes.createIndex("generationLocator", ["generationKey", "key"], { unique: true });
            }
            if (!database.objectStoreNames.contains(STORE.guideLeases)) {
              const leases = database.createObjectStore(STORE.guideLeases, { keyPath: "key" });
              leases.createIndex("generationKey", "generationKey", { unique: false });
              leases.createIndex("expiresAt", "expiresAt", { unique: false });
            }
            if (!database.objectStoreNames.contains(STORE.guideCleanupQueue)) {
              database.createObjectStore(STORE.guideCleanupQueue, { keyPath: "key" })
                .createIndex("cleanupAt", "cleanupAt", { unique: false });
            }
          }
          if (event.oldVersion < 4) {
            if (!database.objectStoreNames.contains(STORE.guideTimeline)) {
              const timeline = database.createObjectStore(STORE.guideTimeline, { keyPath: "key" });
              timeline.createIndex(
                "generationChannelFiniteStart",
                ["generationKey", "channelKey", "finiteStartMs"],
                { unique: true },
              );
              timeline.createIndex(
                "generationChannelOpenStart",
                ["generationKey", "channelKey", "openStartMs"],
                { unique: true },
              );
            }
            if (!database.objectStoreNames.contains(STORE.guideMigration)) {
              database.createObjectStore(STORE.guideMigration, { keyPath: "key" });
            }
            if (event.oldVersion < 3) {
              openRequest.transaction.objectStore(STORE.guideMigration).put({
                key: "legacy-v3",
                afterKey: null,
                pendingGenerationKey: null,
                complete: true,
              });
            }
          }
        };
        openRequest.onblocked = () => {
          blocked = true;
          reject(error("AIR_IDB_BLOCKED"));
        };
        openRequest.onerror = () => reject(error(classify(openRequest.error)));
        openRequest.onsuccess = () => {
          const database = openRequest.result;
          if (blocked) { database.close(); return; }
          database.onversionchange = () => {
            database.close();
            connections.delete(name);
          };
          database.onclose = () => connections.delete(name);
          connections.set(name, database);
          resolve(database);
        };
      });
    };
    const transaction = (database, stores, mode, operationId, body) =>
      new Promise((resolve, reject) => {
        let tx;
        try {
          tx = database.transaction(stores, mode);
        } catch (failure) {
          reject(error(classify(failure)));
          return;
        }
        transactions.set(operationId, tx);
        let result = null;
        let settled = false;
        const fail = (code) => {
          if (!tx.__airFailure) tx.__airFailure = code;
          try { tx.abort(); } catch (_) { /* already completed */ }
        };
        tx.oncomplete = () => {
          transactions.delete(operationId);
          if (!settled) {
            settled = true;
            resolve(stringify(result));
          }
        };
        tx.onabort = () => {
          transactions.delete(operationId);
          if (!settled) {
            settled = true;
            reject(error(tx.__airFailure || classify(tx.error)));
          }
        };
        tx.onerror = (event) => {
          if (event) event.preventDefault();
          fail(classify(tx.error));
        };
        const setResult = (value) => { result = value; };
        try {
          body(tx, setResult, fail);
        } catch (failure) {
          fail(classify(failure));
        }
      });
    const getJson = (store, key, onValue, fail) => {
      const getRequest = store.get(key);
      getRequest.onsuccess = () => {
        try { onValue(parse(getRequest.result)); } catch (_) { fail("AIR_IDB_CORRUPT"); }
      };
      getRequest.onerror = () => fail(classify(getRequest.error));
    };
    const requireWritable = (source, generation, generationRow, fail) => {
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
    const cursorValues = (store, range, direction, limit, filter, done, fail) => {
      const values = [];
      const cursorRequest = store.openCursor(range, direction);
      cursorRequest.onerror = () => fail(classify(cursorRequest.error));
      cursorRequest.onsuccess = () => {
        const cursor = cursorRequest.result;
        if (!cursor || values.length >= limit) {
          done(values);
          return;
        }
        let value;
        try { value = parse(cursor.value); } catch (_) { fail("AIR_IDB_CORRUPT"); return; }
        if (!filter || filter(value)) values.push(value);
        if (values.length >= limit) done(values); else cursor.continue();
      };
    };
    const countPrefix = (store, prefix, done, fail) => {
      const countRequest = store.count(rangeForPrefix(prefix));
      countRequest.onsuccess = () => done(countRequest.result);
      countRequest.onerror = () => fail(classify(countRequest.error));
    };
    const deletePrefixRows = (tx, command, generationRow, setResult, fail) => {
      let remaining = command.maxRows;
      let removed = 0;
      const tasks = [
        [STORE.catalogRecords, command.prefixes.catalogRecord],
        [STORE.channelRecords, command.prefixes.channelRecord],
        [STORE.programmes, command.prefixes.programme, null, null],
        [STORE.counters, command.prefixes.counter, null, null],
      ];
      let taskIndex = 0;
      const runNext = () => {
        if (remaining === 0 || taskIndex >= tasks.length) {
          finishGeneration();
          return;
        }
        const task = tasks[taskIndex++];
        const store = tx.objectStore(task[0]);
        const cursorRequest = store.openCursor(rangeForPrefix(task[1]));
        cursorRequest.onerror = () => fail(classify(cursorRequest.error));
        cursorRequest.onsuccess = () => {
          const cursor = cursorRequest.result;
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
      const finishGeneration = () => {
        const checks = tasks.map((task) => tx.objectStore(task[0]).count(rangeForPrefix(task[1])));
        let completed = 0;
        let total = 0;
        for (const check of checks) {
          check.onerror = () => fail(classify(check.error));
          check.onsuccess = () => {
            total += check.result;
            completed += 1;
            if (completed !== checks.length) return;
            if (total === 0) {
              tx.objectStore(STORE.generations).delete(generationRow.generationKey);
              tx.objectStore(STORE.orphanQueue).delete(command.queueKey);
            }
            const queueCount = tx.objectStore(STORE.orphanQueue).count();
            queueCount.onerror = () => fail(classify(queueCount.error));
            queueCount.onsuccess = () => setResult({ removedRows: removed, hasMore: queueCount.result > 0 || total > 0 });
          };
        }
      };
      runNext();
    };

    const guideSourceState = (key, sourceKey) => ({
      key: key,
      kind: "source",
      sourceKey: sourceKey,
      epoch: 1,
      mutation: 0,
      activeFeedCount: 0,
      stagedOnlyFeedCount: 0,
      deleted: false,
    });
    const guideFeedState = (key, sourceKey, feedId, sourceEpoch) => ({
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
    });
    const guideSnapshot = (feed) => {
      if (!feed || feed.deleted || feed.activeGeneration === null) return null;
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
    const compareNullable = (left, right, compare) => {
      if (left === null && right === null) return 0;
      if (left === null) return -1;
      if (right === null) return 1;
      return compare(left, right);
    };
    const compareStrings = (left, right) => {
      const count = Math.min(left.length, right.length);
      for (let index = 0; index < count; index += 1) {
        if (left[index] < right[index]) return -1;
        if (left[index] > right[index]) return 1;
      }
      return left.length - right.length;
    };
    const compareGuideChannels = (left, right) => {
      let comparison = compareStrings(left.displayNames, right.displayNames);
      if (comparison !== 0) return comparison;
      return compareNullable(left.artworkReference, right.artworkReference, (a, b) => a < b ? -1 : (a > b ? 1 : 0));
    };
    const compareGuideProgrammes = (left, right) => {
      const stringCompare = (a, b) => a < b ? -1 : (a > b ? 1 : 0);
      let comparison = compareNullable(left.endMs, right.endMs, (a, b) => a - b);
      if (comparison !== 0) return comparison;
      comparison = stringCompare(left.title, right.title);
      if (comparison !== 0) return comparison;
      comparison = compareNullable(left.subtitle, right.subtitle, stringCompare);
      if (comparison !== 0) return comparison;
      comparison = compareNullable(left.description, right.description, stringCompare);
      if (comparison !== 0) return comparison;
      comparison = compareStrings(left.categories, right.categories);
      if (comparison !== 0) return comparison;
      comparison = compareNullable(left.artworkReference, right.artworkReference, stringCompare);
      if (comparison !== 0) return comparison;
      return compareNullable(left.episode, right.episode, stringCompare);
    };
    const encodedFieldBytes = (value) => 16 + (value === null ? 0 : new TextEncoder().encode(value).length);
    const guideProgrammeBytes = (programme) =>
      16 + encodedFieldBytes(programme.channelKey) + encodedFieldBytes(programme.winnerKey) +
      encodedFieldBytes(programme.title) + encodedFieldBytes(programme.subtitle) +
      encodedFieldBytes(programme.description) + encodedFieldBytes(programme.episode) +
      encodedFieldBytes(programme.artworkReference) + 4 +
      programme.categories.reduce((total, category) => total + encodedFieldBytes(category), 0);
    const guideTimelineRow = (programme) => {
      const row = {
        key: programme.key,
        generationKey: programme.generationKey,
        channelKey: programme.channelKey,
        startMs: programme.startMs,
        effectiveEndMs: programme.effectiveEndMs,
      };
      if (programme.endMs === null) row.openStartMs = programme.startMs;
      else row.finiteStartMs = programme.startMs;
      return row;
    };
    const guideQueuePut = (tx, generationKey, cleanupAt, kind = "generation") => {
      tx.objectStore(STORE.guideCleanupQueue).put({
        key: "Q|" + generationKey,
        generationKey: generationKey,
        kind: kind,
        cleanupAt: cleanupAt,
      });
    };
    const guideLease = (tx, command, onValid, setResult, fail) => {
      const leases = tx.objectStore(STORE.guideLeases);
      if (!command.leaseKey) { fail("AIR_IDB_CORRUPT:missing-lease-key"); return; }
      let request;
      try { request = leases.get(command.leaseKey); } catch (failure) {
        fail("AIR_IDB_CORRUPT");
        return;
      }
      request.onerror = () => fail(classify(request.error));
      request.onsuccess = () => {
        const lease = request.result;
        if (!lease || lease.ownerId !== command.ownerId || lease.expiresAt <= command.nowMs) {
          setResult({ status: "stale" });
          return;
        }
        if (!lease.generationKey) { fail("AIR_IDB_CORRUPT:missing-lease-generation-key"); return; }
        let generationRequest;
        try { generationRequest = tx.objectStore(STORE.guideGenerations).get(lease.generationKey); } catch (failure) {
          fail("AIR_IDB_CORRUPT");
          return;
        }
        generationRequest.onerror = () => fail(classify(generationRequest.error));
        generationRequest.onsuccess = () => {
          const generation = generationRequest.result;
          if (!generation || generation.cleanupStarted) {
            setResult({ status: "stale" });
            return;
          }
          onValid(lease, generation);
        };
      };
    };
    const guideCursorPage = (store, range, direction, afterKey, limit, mapValue, done, fail) => {
      const rows = [];
      const lower = afterKey === null ? range.lower : afterKey;
      const actualRange = IDBKeyRange.bound(lower, range.upper, afterKey !== null, false);
      const request = store.openCursor(actualRange, direction || "next");
      request.onerror = () => fail(classify(request.error));
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          done(rows, null);
          return;
        }
        if (rows.length >= limit) {
          done(rows, rows.length === 0 ? null : rows[rows.length - 1].cursorKey);
          return;
        }
        const mapped = mapValue(cursor.value, cursor.key);
        if (mapped !== null) rows.push(mapped);
        cursor.continue();
      };
    };

    const execute = (name, text) => {
      let command;
      try { command = JSON.parse(text); } catch (_) { throw error("AIR_IDB_COMMAND"); }
      if (command.op === "cancel") {
        const active = transactions.get(command.targetOperationId);
        if (active) {
          active.__airFailure = "AIR_IDB_CANCELLED";
          try { active.abort(); } catch (_) { /* already completed */ }
        }
        return Promise.resolve("null");
      }
      if (command.op === "close") {
        const database = connections.get(name);
        if (database) database.close();
        connections.delete(name);
        return Promise.resolve("null");
      }
      return open(name).then((database) => {
        switch (command.op) {
        case "open":
          return "null";
        case "begin":
          return transaction(database, [STORE.sources, STORE.generations, STORE.orphanQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const sources = tx.objectStore(STORE.sources);
            getJson(sources, command.sourceKey, (existing) => {
              const source = existing || { activeGeneration: null, nextGeneration: 1, revision: 0, activatedAtMs: null, deleted: false };
              const generation = source.nextGeneration;
              if (!Number.isSafeInteger(generation) || generation <= 0) { fail("AIR_IDB_GENERATION_EXHAUSTED"); return; }
              const generationComponent = sortableKey(generation);
              const generationKey = command.generationPrefix + generationComponent;
              const queueKey = command.queuePrefix + generationComponent;
              source.nextGeneration = generation + 1;
              source.deleted = false;
              sources.put(stringify(source), command.sourceKey);
              tx.objectStore(STORE.generations).put(stringify({
                stagedAtMs: command.nowMs,
                queueKey: queueKey,
                generationKey: generationKey,
                sourceComponent: command.sourceComponent,
                generationComponent: generationComponent,
              }), generationKey);
              tx.objectStore(STORE.orphanQueue).put(stringify({ generationKey: generationKey }), queueKey);
              setResult({ generation: generation });
            }, fail);
          });
        case "stageCatalog":
          return transaction(database, [STORE.sources, STORE.generations, STORE.catalogRecords, STORE.counters], "readwrite", command.operationId, (tx, setResult, fail) => {
            let source = null;
            let generationRow = null;
            let counter = null;
            let ready = 0;
            const advance = () => {
              ready += 1;
              if (ready !== 3) return;
              if (!requireWritable(source, command.generation, generationRow, fail)) return;
              let order = counter && Number.isSafeInteger(counter.nextOrder) ? counter.nextOrder : 0;
              let index = 0;
              const records = tx.objectStore(STORE.catalogRecords);
              const writeNext = () => {
                if (index >= command.rows.length) {
                  tx.objectStore(STORE.counters).put(stringify({ nextOrder: order }), command.counterKey);
                  setResult(null);
                  return;
                }
                const row = command.rows[index++];
                getJson(records, row.recordKey, (old) => {
                  row.sortOrder = order;
                  row.orderKey = command.orderPrefix + sortableKey(order) + "|" + row.entityKey;
                  order += 1;
                  records.put(row);
                  writeNext();
                }, fail);
              };
              writeNext();
            };
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (value) => { source = value; advance(); }, fail);
            getJson(tx.objectStore(STORE.generations), command.generationKey, (value) => { generationRow = value; advance(); }, fail);
            getJson(tx.objectStore(STORE.counters), command.counterKey, (value) => { counter = value; advance(); }, fail);
          });
        case "stageGuide":
          return transaction(database, [STORE.sources, STORE.generations, STORE.channelRecords, STORE.programmes, STORE.counters], "readwrite", command.operationId, (tx, setResult, fail) => {
            let source = null;
            let generationRow = null;
            let counter = null;
            let ready = 0;
            const advance = () => {
              ready += 1;
              if (ready !== 3) return;
              if (!requireWritable(source, command.generation, generationRow, fail)) return;
              let order = counter && Number.isSafeInteger(counter.nextOrder) ? counter.nextOrder : 0;
              let index = 0;
              const records = tx.objectStore(STORE.channelRecords);
              const writeNextChannel = () => {
                if (index >= command.channels.length) {
                  tx.objectStore(STORE.counters).put(stringify({ nextOrder: order }), command.counterKey);
                  for (const programme of command.programmes) tx.objectStore(STORE.programmes).put(stringify(programme), programme.recordKey);
                  setResult(null);
                  return;
                }
                const row = command.channels[index++];
                getJson(records, row.recordKey, (old) => {
                  row.sortOrder = order;
                  row.orderKey = command.orderPrefix + sortableKey(order) + "|" + row.channelKey;
                  order += 1;
                  records.put(row);
                  writeNextChannel();
                }, fail);
              };
              writeNextChannel();
            };
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (value) => { source = value; advance(); }, fail);
            getJson(tx.objectStore(STORE.generations), command.generationKey, (value) => { generationRow = value; advance(); }, fail);
            getJson(tx.objectStore(STORE.counters), command.counterKey, (value) => { counter = value; advance(); }, fail);
          });
        case "activate":
          return transaction(database, [STORE.sources, STORE.generations, STORE.orphanQueue, STORE.catalogRecords, STORE.channelRecords, STORE.programmes], "readwrite", command.operationId, (tx, setResult, fail) => {
            let source = null;
            let generationRow = null;
            let catalogCount = null;
            let channelCount = null;
            let programmeCount = null;
            let ready = 0;
            const advance = () => {
              ready += 1;
              if (ready !== 5) return;
              if (!requireWritable(source, command.generation, generationRow, fail)) return;
              if (catalogCount !== command.expected.catalogItems || channelCount !== command.expected.channels || programmeCount !== command.expected.programmes) {
                fail("AIR_IDB_COUNT_MISMATCH");
                return;
              }
              const oldGeneration = source.activeGeneration;
              source.activeGeneration = command.generation;
              source.revision += 1;
              source.activatedAtMs = command.nowMs;
              source.deleted = false;
              tx.objectStore(STORE.sources).put(stringify(source), command.sourceKey);
              tx.objectStore(STORE.orphanQueue).delete(generationRow.queueKey);
              if (oldGeneration !== null && oldGeneration !== command.generation) {
                const oldKey = command.generationPrefix + sortableKey(oldGeneration);
                const oldQueueKey = "Q|" + command.nowKey + "|" + command.sourceComponent + "|" + sortableKey(oldGeneration);
                getJson(tx.objectStore(STORE.generations), oldKey, (oldRow) => {
                  if (oldRow) {
                    oldRow.queueKey = oldQueueKey;
                    tx.objectStore(STORE.generations).put(stringify(oldRow), oldKey);
                    tx.objectStore(STORE.orphanQueue).put(stringify({ generationKey: oldKey }), oldQueueKey);
                  }
                }, fail);
              }
              setResult(source);
            };
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (value) => { source = value; advance(); }, fail);
            getJson(tx.objectStore(STORE.generations), command.generationKey, (value) => { generationRow = value; advance(); }, fail);
            countPrefix(tx.objectStore(STORE.catalogRecords), command.prefixes.catalogRecord, (value) => { catalogCount = value; advance(); }, fail);
            countPrefix(tx.objectStore(STORE.channelRecords), command.prefixes.channelRecord, (value) => { channelCount = value; advance(); }, fail);
            countPrefix(tx.objectStore(STORE.programmes), command.prefixes.programme, (value) => { programmeCount = value; advance(); }, fail);
          });
        case "status":
          return transaction(database, [STORE.sources], "readonly", command.operationId, (tx, setResult, fail) => {
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (source) => setResult(source && !source.deleted ? source : null), fail);
          });
        case "catalogPage":
        case "channelPage":
          return transaction(database, [STORE.sources, command.op === "catalogPage" ? STORE.catalogRecords : STORE.channelRecords], "readonly", command.operationId, (tx, setResult, fail) => {
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (source) => {
              if (!source || source.deleted || source.activeGeneration === null) { setResult([]); return; }
              const prefix = command.activePrefix + sortableKey(source.activeGeneration) + command.tailPrefix;
              const lower = command.afterKey === null ? prefix : prefix + command.afterKey + "|\uffff";
              const range = IDBKeyRange.bound(lower, prefix + "\uffff", command.afterKey !== null, false);
              cursorValues(
                tx.objectStore(command.op === "catalogPage" ? STORE.catalogRecords : STORE.channelRecords).index("orderKey"),
                range,
                "next",
                command.limit,
                null,
                setResult,
                fail,
              );
            }, fail);
          });
        case "guideWindow":
          return transaction(database, [STORE.sources, STORE.programmes], "readonly", command.operationId, (tx, setResult, fail) => {
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (source) => {
              if (!source || source.deleted || source.activeGeneration === null) { setResult([]); return; }
              const prefix = command.activePrefix + sortableKey(source.activeGeneration) + command.tailPrefix;
              const range = IDBKeyRange.bound(prefix, prefix + command.untilKey, false, true);
              cursorValues(tx.objectStore(STORE.programmes), range, "next", command.limit, (row) => row.endMs > command.fromMs, setResult, fail);
            }, fail);
          });
        case "nowNext":
          return transaction(database, [STORE.sources, STORE.programmes], "readonly", command.operationId, (tx, setResult, fail) => {
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (source) => {
              if (!source || source.deleted || source.activeGeneration === null) { setResult({ current: null, next: null }); return; }
              const prefix = command.activePrefix + sortableKey(source.activeGeneration) + command.tailPrefix;
              let current = undefined;
              let next = undefined;
              const complete = () => { if (current !== undefined && next !== undefined) setResult({ current: current, next: next }); };
              const currentRequest = tx.objectStore(STORE.programmes).openCursor(IDBKeyRange.bound(prefix, prefix + command.atKey + "|\uffff"), "prev");
              currentRequest.onerror = () => fail(classify(currentRequest.error));
              currentRequest.onsuccess = () => {
                const cursor = currentRequest.result;
                if (!cursor) { current = null; complete(); return; }
                let row;
                try { row = parse(cursor.value); } catch (_) { fail("AIR_IDB_CORRUPT"); return; }
                if (row.endMs > command.atMs) { current = row; complete(); } else cursor.continue();
              };
              const nextRequest = tx.objectStore(STORE.programmes).openCursor(IDBKeyRange.bound(prefix + command.atKey + "|\uffff", prefix + "\uffff", true, false), "next");
              nextRequest.onerror = () => fail(classify(nextRequest.error));
              nextRequest.onsuccess = () => {
                const cursor = nextRequest.result;
                if (!cursor) { next = null; complete(); return; }
                try { next = parse(cursor.value); } catch (_) { fail("AIR_IDB_CORRUPT"); return; }
                complete();
              };
            }, fail);
          });
        case "guideMigrateLegacy":
          return transaction(database, [STORE.guideGenerations, STORE.guideProgrammes, STORE.guideTimeline, STORE.guideMigration], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generations = tx.objectStore(STORE.guideGenerations);
            const programmes = tx.objectStore(STORE.guideProgrammes);
            const timeline = tx.objectStore(STORE.guideTimeline);
            const migrations = tx.objectStore(STORE.guideMigration);
            const stateRequest = migrations.get("legacy-v3");
            stateRequest.onerror = () => fail(classify(stateRequest.error));
            stateRequest.onsuccess = () => {
              const state = stateRequest.result || {
                key: "legacy-v3",
                afterKey: null,
                pendingGenerationKey: null,
                complete: false,
              };
              if (state.complete) {
                setResult({ status: "ok", migratedRows: 0, hasMore: false });
                return;
              }
              let migratedRows = 0;
              const finishGeneration = (generationKey, done) => {
                if (generationKey === null) { done(); return; }
                const request = generations.get(generationKey);
                request.onerror = () => fail(classify(request.error));
                request.onsuccess = () => {
                  const generation = request.result;
                  if (generation) {
                    generation.timelineMigrated = true;
                    generations.put(generation);
                  }
                  done();
                };
              };
              const range = state.afterKey === null ? null : IDBKeyRange.lowerBound(state.afterKey, true);
              const cursorRequest = programmes.openCursor(range);
              cursorRequest.onerror = () => fail(classify(cursorRequest.error));
              cursorRequest.onsuccess = () => {
                const cursor = cursorRequest.result;
                if (!cursor) {
                  finishGeneration(state.pendingGenerationKey, () => {
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
                const programme = cursor.value;
                const processRow = () => {
                  state.pendingGenerationKey = programme.generationKey;
                  const generationRequest = generations.get(programme.generationKey);
                  generationRequest.onerror = () => fail(classify(generationRequest.error));
                  generationRequest.onsuccess = () => {
                    const generation = generationRequest.result;
                    if (generation) {
                      if (!Number.isSafeInteger(generation.maxFiniteSpanMs)) generation.maxFiniteSpanMs = 0;
                      if (!Number.isSafeInteger(generation.minStartMs)) generation.minStartMs = programme.startMs;
                      else generation.minStartMs = Math.min(generation.minStartMs, programme.startMs);
                      if (programme.endMs !== null) {
                        generation.maxFiniteSpanMs = Math.max(
                          generation.maxFiniteSpanMs,
                          programme.effectiveEndMs - programme.startMs,
                        );
                      }
                      generations.put(generation);
                    }
                    timeline.put(guideTimelineRow(programme));
                    state.afterKey = cursor.primaryKey;
                    migratedRows += 1;
                    cursor.continue();
                  };
                };
                if (
                  state.pendingGenerationKey !== null &&
                  state.pendingGenerationKey !== programme.generationKey
                ) {
                  const previous = state.pendingGenerationKey;
                  finishGeneration(previous, processRow);
                } else {
                  processRow();
                }
              };
            };
          });
        case "guideBegin":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            let source = null;
            let feed = null;
            let ready = 0;
            const finish = () => {
              ready += 1;
              if (ready !== 2) return;
              source = source || guideSourceState(command.sourceStateKey, command.sourceKey);
              if (source.deleted) {
                source.deleted = false;
                source.activeFeedCount = 0;
                source.stagedOnlyFeedCount = 0;
              }
              feed = feed || guideFeedState(command.feedStateKey, command.sourceKey, command.feedId, source.epoch);
              if (feed.sourceEpoch !== source.epoch) {
                feed.sourceEpoch = source.epoch;
                feed.activeGeneration = null;
                feed.latestGeneration = null;
                feed.counts = { channels: 0, programmes: 0 };
                feed.retention = null;
                feed.deleted = false;
                delete feed.activeFeedKey;
              }
              const allocate = () => {
                const generation = feed.nextGeneration;
                if (!Number.isSafeInteger(generation) || generation <= 0) { fail("AIR_IDB_GENERATION_EXHAUSTED"); return; }
                const component = sortableKey(generation);
                const generationKey = command.generationPrefix + component;
                const wasStagedOnly = feed.activeGeneration === null && feed.latestGeneration !== null;
                if (feed.activeGeneration === null && !wasStagedOnly) source.stagedOnlyFeedCount += 1;
                feed.nextGeneration = generation + 1;
                feed.latestGeneration = generation;
                feed.mutation += 1;
                feed.deleted = false;
                source.mutation += 1;
                const generationRow = {
                  key: generationKey,
                  sourceKey: command.sourceKey,
                  sourceStateKey: command.sourceStateKey,
                  feedId: command.feedId,
                  sourceEpoch: source.epoch,
                  sourceEpochKey: command.sourceKey + "|" + sortableKey(source.epoch),
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
                tx.objectStore(STORE.guideGenerations).put(generationRow);
                guideQueuePut(tx, generationKey, generationRow.expiresAt);
                setResult({ status: "ok", generation: generation, mutationEpoch: feed.mutation, sourceEpoch: source.epoch });
              };
              if (feed.latestGeneration === null) {
                allocate();
              } else {
                const oldKey = command.generationPrefix + sortableKey(feed.latestGeneration);
                const oldRequest = tx.objectStore(STORE.guideGenerations).get(oldKey);
                oldRequest.onerror = () => fail(classify(oldRequest.error));
                oldRequest.onsuccess = () => {
                  const old = oldRequest.result;
                  if (old) {
                    old.status = "superseded";
                    old.expiresAt = 0;
                    tx.objectStore(STORE.guideGenerations).put(old);
                    guideQueuePut(tx, old.key, 0);
                  }
                  allocate();
                };
              }
            };
            const sourceRequest = states.get(command.sourceStateKey);
            sourceRequest.onerror = () => fail(classify(sourceRequest.error));
            sourceRequest.onsuccess = () => { source = sourceRequest.result; finish(); };
            const feedRequest = states.get(command.feedStateKey);
            feedRequest.onerror = () => fail(classify(feedRequest.error));
            feedRequest.onsuccess = () => { feed = feedRequest.result; finish(); };
          });
        case "guideRenewGeneration":
        case "guideAbandon":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generationRequest = tx.objectStore(STORE.guideGenerations).get(command.generationKey);
            generationRequest.onerror = () => fail(classify(generationRequest.error));
            generationRequest.onsuccess = () => {
              const generation = generationRequest.result;
              const abandonablePoison = command.op === "guideAbandon" && generation && generation.status === "poisoned";
              if (!generation || (generation.status !== "staging" && !abandonablePoison)) {
                setResult({ status: "terminal", value: false }); return;
              }
              const states = tx.objectStore(STORE.guideStates);
              const sourceRequest = states.get(generation.sourceStateKey);
              sourceRequest.onerror = () => fail(classify(sourceRequest.error));
              sourceRequest.onsuccess = () => {
                const source = sourceRequest.result;
                if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                  setResult({ status: "terminal", value: false }); return;
                }
                const feedRequest = states.get(generation.feedStateKey);
                feedRequest.onerror = () => fail(classify(feedRequest.error));
                feedRequest.onsuccess = () => {
                  const feed = feedRequest.result;
                  if (
                    !feed || feed.sourceEpoch !== generation.sourceEpoch ||
                    feed.latestGeneration !== generation.generation ||
                    (generation.status === "staging" && generation.expiresAt <= command.nowMs)
                  ) {
                    setResult({ status: "terminal", value: false });
                    return;
                  }
                  if (command.op === "guideRenewGeneration") {
                    generation.expiresAt = command.nowMs + command.generationIdleTimeoutMillis;
                    tx.objectStore(STORE.guideGenerations).put(generation);
                    guideQueuePut(tx, generation.key, generation.expiresAt);
                    setResult({ status: "ok", value: true });
                  } else {
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
                    tx.objectStore(STORE.guideGenerations).put(generation);
                    guideQueuePut(tx, generation.key, 0);
                    setResult({ status: "ok", value: true });
                  }
                };
              };
            };
          });
        case "guideRejectStage":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generations = tx.objectStore(STORE.guideGenerations);
            const request = generations.get(command.generationKey);
            request.onerror = () => fail(classify(request.error));
            request.onsuccess = () => {
              const generation = request.result;
              if (!generation) { setResult({ status: "stale" }); return; }
              if (generation.status === "poisoned") { setResult({ status: "limit" }); return; }
              if (generation.status !== "staging" || generation.expiresAt <= command.nowMs) {
                setResult({ status: "stale" }); return;
              }
              const states = tx.objectStore(STORE.guideStates);
              const sourceRequest = states.get(generation.sourceStateKey);
              sourceRequest.onerror = () => fail(classify(sourceRequest.error));
              sourceRequest.onsuccess = () => {
                const source = sourceRequest.result;
                if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                  setResult({ status: "stale" }); return;
                }
                const feedRequest = states.get(generation.feedStateKey);
                feedRequest.onerror = () => fail(classify(feedRequest.error));
                feedRequest.onsuccess = () => {
                  const feed = feedRequest.result;
                  if (!feed || feed.sourceEpoch !== generation.sourceEpoch || feed.latestGeneration !== generation.generation) {
                    setResult({ status: "stale" }); return;
                  }
                  generation.batchCount += 1;
                  generation.inputChannelRows += command.inputChannelRows;
                  generation.inputProgrammeRows += command.inputProgrammeRows;
                  generation.status = "poisoned";
                  generation.expiresAt = 0;
                  generations.put(generation);
                  guideQueuePut(tx, generation.key, 0);
                  setResult({ status: "limit" });
                };
              };
            };
          });
        case "guideStage":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideChannels, STORE.guideProgrammes, STORE.guideTimeline, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generations = tx.objectStore(STORE.guideGenerations);
            const generationRequest = generations.get(command.generationKey);
            generationRequest.onerror = () => fail(classify(generationRequest.error));
            generationRequest.onsuccess = () => {
              const generation = generationRequest.result;
              if (generation && generation.status === "poisoned") { setResult({ status: "limit" }); return; }
              if (!generation || generation.status !== "staging" || generation.expiresAt <= command.nowMs) {
                setResult({ status: "stale" }); return;
              }
              const states = tx.objectStore(STORE.guideStates);
              const sourceRequest = states.get(generation.sourceStateKey);
              sourceRequest.onerror = () => fail(classify(sourceRequest.error));
              sourceRequest.onsuccess = () => {
                const source = sourceRequest.result;
                if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                  setResult({ status: "stale" }); return;
                }
                const feedRequest = states.get(generation.feedStateKey);
                feedRequest.onerror = () => fail(classify(feedRequest.error));
                feedRequest.onsuccess = () => {
                const feed = feedRequest.result;
                if (!feed || feed.latestGeneration !== generation.generation) { setResult({ status: "stale" }); return; }
                const batchItems = command.channels.length + command.programmes.length;
                generation.batchCount += 1;
                generation.inputChannelRows += command.channels.length;
                generation.inputProgrammeRows += command.programmes.length;
                if (
                  generation.batchCount > command.maxBatches ||
                  generation.inputChannelRows > command.maxInputChannels ||
                  generation.inputProgrammeRows > command.maxInputProgrammes ||
                  batchItems > command.maxBatchItems
                ) {
                  generation.status = "poisoned";
                  generation.expiresAt = 0;
                  generations.put(generation);
                  guideQueuePut(tx, generation.key, 0);
                  setResult({ status: "limit" });
                  return;
                }
                const channelsStore = tx.objectStore(STORE.guideChannels);
                const programmesStore = tx.objectStore(STORE.guideProgrammes);
                const channelUpdates = [];
                const programmeUpdates = [];
                let pending = command.channels.length + command.programmes.length;
                let addedChannels = 0;
                let addedProgrammes = 0;
                const complete = () => {
                  if (pending !== 0) return;
                  const channelCount = generation.channelCount + addedChannels;
                  const programmeCount = generation.programmeCount + addedProgrammes;
                  if (channelCount > command.maxChannels || programmeCount > command.maxProgrammes) {
                    generation.status = "poisoned";
                    generation.expiresAt = 0;
                    generations.put(generation);
                    guideQueuePut(tx, generation.key, 0);
                    setResult({ status: "limit" });
                    return;
                  }
                  channelUpdates.forEach((row) => channelsStore.put(row));
                  const timelineStore = tx.objectStore(STORE.guideTimeline);
                  programmeUpdates.forEach((row) => {
                    programmesStore.put(row);
                    timelineStore.put(guideTimelineRow(row));
                  });
                  generation.channelCount = channelCount;
                  generation.programmeCount = programmeCount;
                  generation.expiresAt = command.nowMs + command.generationIdleTimeoutMillis;
                  generations.put(generation);
                  guideQueuePut(tx, generation.key, generation.expiresAt);
                  setResult({ status: "ok", counts: { channels: channelCount, programmes: programmeCount } });
                };
                if (pending === 0) complete();
                command.channels.forEach((candidate) => {
                  candidate.sourceKey = generation.sourceKey;
                  candidate.feedId = generation.feedId;
                  candidate.generation = generation.generation;
                  candidate.generationKey = generation.key;
                  const request = channelsStore.get(candidate.key);
                  request.onerror = () => fail(classify(request.error));
                  request.onsuccess = () => {
                    const current = request.result;
                    if (!current) addedChannels += 1;
                    if (!current || compareGuideChannels(candidate, current) < 0) channelUpdates.push(candidate);
                    pending -= 1; complete();
                  };
                });
                command.programmes.forEach((candidate) => {
                  candidate.sourceKey = generation.sourceKey;
                  candidate.feedId = generation.feedId;
                  candidate.generation = generation.generation;
                  candidate.generationKey = generation.key;
                  generation.minStartMs = generation.minStartMs === null
                    ? candidate.startMs
                    : Math.min(generation.minStartMs, candidate.startMs);
                  if (candidate.endMs === null) candidate.openStartMs = candidate.startMs;
                  else {
                    candidate.finiteStartMs = candidate.startMs;
                    generation.maxFiniteSpanMs = Math.max(
                      generation.maxFiniteSpanMs,
                      candidate.effectiveEndMs - candidate.startMs,
                    );
                  }
                  const request = programmesStore.get(candidate.key);
                  request.onerror = () => fail(classify(request.error));
                  request.onsuccess = () => {
                    const current = request.result;
                    if (!current) addedProgrammes += 1;
                    if (!current || compareGuideProgrammes(candidate, current) < 0) programmeUpdates.push(candidate);
                    pending -= 1; complete();
                  };
                });
                };
              };
            };
          });
        case "guideActivate":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const generations = tx.objectStore(STORE.guideGenerations);
            const generationRequest = generations.get(command.generationKey);
            generationRequest.onerror = () => fail(classify(generationRequest.error));
            generationRequest.onsuccess = () => {
              const generation = generationRequest.result;
              if (!generation) { setResult({ status: "stale" }); return; }
              if (generation.status === "poisoned") { setResult({ status: "limit" }); return; }
              const feedRequest = states.get(generation.feedStateKey);
              feedRequest.onerror = () => fail(classify(feedRequest.error));
              feedRequest.onsuccess = () => {
                const feed = feedRequest.result;
                if (!feed || feed.latestGeneration !== generation.generation || generation.status !== "staging") {
                  setResult({ status: "superseded", current: guideSnapshot(feed) }); return;
                }
                if (generation.expiresAt <= command.nowMs) { setResult({ status: "stale" }); return; }
                if (
                  generation.channelCount !== command.expected.channels ||
                  generation.programmeCount !== command.expected.programmes
                ) { setResult({ status: "corrupt" }); return; }
                if (generation.channelCount === 0 && generation.programmeCount === 0) {
                  setResult({ status: "limit" }); return;
                }
                const sourceRequest = states.get(command.sourceStateKey);
                sourceRequest.onerror = () => fail(classify(sourceRequest.error));
                sourceRequest.onsuccess = () => {
                  const source = sourceRequest.result;
                  if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                    setResult({ status: "superseded", current: null }); return;
                  }
                  const oldGeneration = feed.activeGeneration;
                  const hadActive = oldGeneration !== null;
                  feed.activeGeneration = generation.generation;
                  feed.latestGeneration = null;
                  feed.revision += 1;
                  feed.mutation = generation.mutationEpoch;
                  feed.counts = { channels: generation.channelCount, programmes: generation.programmeCount };
                  feed.retention = generation.retention;
                  feed.deleted = false;
                  feed.activeFeedKey = command.activeFeedBase + sortableKey(source.epoch) + "|" + command.feedComponent;
                  generation.status = "active";
                  generations.put(generation);
                  tx.objectStore(STORE.guideCleanupQueue).delete("Q|" + generation.key);
                  if (oldGeneration !== null && oldGeneration !== generation.generation) {
                    const oldKey = command.generationPrefix + sortableKey(oldGeneration);
                    const oldRequest = generations.get(oldKey);
                    oldRequest.onsuccess = () => {
                      const old = oldRequest.result;
                      if (old) {
                        old.status = "inactive";
                        old.expiresAt = 0;
                        generations.put(old);
                        guideQueuePut(tx, old.key, 0);
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
                  setResult({ status: "published", snapshot: guideSnapshot(feed) });
                };
              };
            };
          });
        case "guideSnapshot":
          return transaction(database, [STORE.guideStates], "readonly", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const sourceRequest = states.get(command.sourceStateKey);
            sourceRequest.onerror = () => fail(classify(sourceRequest.error));
            sourceRequest.onsuccess = () => {
              const source = sourceRequest.result;
              if (!source || source.deleted) { setResult(null); return; }
              const feedRequest = states.get(command.feedStateKey);
              feedRequest.onerror = () => fail(classify(feedRequest.error));
              feedRequest.onsuccess = () => {
                const feed = feedRequest.result;
                setResult(feed && feed.sourceEpoch === source.epoch ? guideSnapshot(feed) : null);
              };
            };
          });
        case "guideSourceSnapshot":
          return transaction(database, [STORE.guideStates], "readonly", command.operationId, (tx, setResult, fail) => {
            const request = tx.objectStore(STORE.guideStates).get(command.sourceStateKey);
            request.onerror = () => fail(classify(request.error));
            request.onsuccess = () => {
              const source = request.result || guideSourceState(command.sourceStateKey, command.sourceKey);
              setResult({ status: "ok", sourceKey: command.sourceKey, epoch: source.epoch, mutation: source.mutation, feedCount: source.deleted ? 0 : source.activeFeedCount });
            };
          });
        case "guideSnapshots":
          return transaction(database, [STORE.guideStates], "readonly", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const sourceRequest = states.get(command.sourceStateKey);
            sourceRequest.onerror = () => fail(classify(sourceRequest.error));
            sourceRequest.onsuccess = () => {
              const source = sourceRequest.result;
              if (!source || source.epoch !== command.sourceEpoch || source.mutation !== command.sourceMutation) {
                setResult({ status: "stale" }); return;
              }
              const prefix = command.activeFeedPrefix;
              const lower = command.afterKey === null ? prefix : command.afterKey;
              const range = IDBKeyRange.bound(lower, prefix + "\uffff", command.afterKey !== null, false);
              const rows = [];
              const cursorRequest = states.index("activeFeedKey").openCursor(range);
              cursorRequest.onerror = () => fail(classify(cursorRequest.error));
              cursorRequest.onsuccess = () => {
                const cursor = cursorRequest.result;
                if (!cursor || rows.length >= command.limit) {
                  setResult({ status: "ok", rows: rows, nextKey: cursor ? rows[rows.length - 1].activeFeedKey : null });
                  return;
                }
                const feed = cursor.value;
                if (feed.sourceEpoch === source.epoch && !feed.deleted) rows.push(Object.assign(guideSnapshot(feed), { activeFeedKey: feed.activeFeedKey }));
                cursor.continue();
              };
            };
          });
        case "guideAcquire":
          return transaction(database, [STORE.guideGenerations, STORE.guideLeases], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generations = tx.objectStore(STORE.guideGenerations);
            const generationRequest = generations.get(command.generationKey);
            generationRequest.onerror = () => fail(classify(generationRequest.error));
            generationRequest.onsuccess = () => {
              const generation = generationRequest.result;
              if (
                !generation || generation.cleanupStarted || generation.generation !== command.generation
              ) { setResult({ status: "missing" }); return; }
              const leases = tx.objectStore(STORE.guideLeases);
              const expiry = leases.index("expiresAt");
              const expiredRequest = expiry.openCursor(IDBKeyRange.upperBound(command.nowMs));
              let liveCount = 0;
              expiredRequest.onerror = () => fail(classify(expiredRequest.error));
              expiredRequest.onsuccess = () => {
                const cursor = expiredRequest.result;
                if (cursor) { cursor.delete(); cursor.continue(); return; }
                const countRequest = leases.count();
                countRequest.onerror = () => fail(classify(countRequest.error));
                countRequest.onsuccess = () => {
                  liveCount = countRequest.result;
                  if (liveCount >= command.maxLiveLeases) { setResult({ status: "limit" }); return; }
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
          return transaction(database, [STORE.guideLeases, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const leases = tx.objectStore(STORE.guideLeases);
            const request = leases.get(command.leaseKey);
            request.onerror = () => fail(classify(request.error));
            request.onsuccess = () => {
              const lease = request.result;
              if (!lease || lease.ownerId !== command.ownerId) { setResult({ status: "ok", value: false }); return; }
              if (command.op === "guideReleaseLease") {
                leases.delete(lease.key);
                const queue = tx.objectStore(STORE.guideCleanupQueue);
                const queueRequest = queue.get("Q|" + lease.generationKey);
                queueRequest.onerror = () => fail(classify(queueRequest.error));
                queueRequest.onsuccess = () => {
                  const queueRow = queueRequest.result;
                  if (queueRow) { queueRow.cleanupAt = 0; queue.put(queueRow); }
                  setResult({ status: "ok", value: true });
                };
              } else if (lease.expiresAt <= command.nowMs) {
                leases.delete(lease.key);
                setResult({ status: "ok", value: false });
              } else {
                lease.expiresAt = command.nowMs + command.leaseIdleTimeoutMillis;
                leases.put(lease);
                setResult({ status: "ok", value: true });
              }
            };
          });
        case "guideChannels":
          return transaction(database, [STORE.guideLeases, STORE.guideGenerations, STORE.guideChannels], "readonly", command.operationId, (tx, setResult, fail) => {
            guideLease(tx, command, (_lease, generation) => {
              const prefix = command.channelPrefix;
              guideCursorPage(
                tx.objectStore(STORE.guideChannels),
                { lower: prefix, upper: prefix + "\uffff" },
                "next",
                command.afterKey,
                command.limit,
                (row, key) => Object.assign({ cursorKey: key }, row),
                (rows, nextKey) => setResult({ status: "ok", rows: rows, nextKey: nextKey }),
                fail,
              );
            }, setResult, fail);
          });
        case "guideSearchRows":
        case "guideFullProgrammes":
          return transaction(database, [STORE.guideLeases, STORE.guideGenerations, STORE.guideProgrammes], "readonly", command.operationId, (tx, setResult, fail) => {
            guideLease(tx, command, (_lease, generation) => {
              const prefix = command.programmePrefix;
              guideCursorPage(
                tx.objectStore(STORE.guideProgrammes),
                { lower: prefix, upper: prefix + "\uffff" },
                "next",
                command.afterKey,
                command.limit,
                (row, key) => command.op === "guideSearchRows"
                  ? {
                      cursorKey: key,
                      locatorKey: key,
                      startMs: row.startMs,
                      effectiveEndMs: row.effectiveEndMs,
                      title: row.title,
                      subtitle: row.subtitle,
                    }
                  : Object.assign({ cursorKey: key, locatorKey: key }, row),
                (rows, nextKey) => setResult({ status: "ok", rows: rows, nextKey: nextKey }),
                fail,
              );
            }, setResult, fail);
          });
        case "guideProgramme":
          return transaction(database, [STORE.guideLeases, STORE.guideGenerations, STORE.guideProgrammes], "readonly", command.operationId, (tx, setResult, fail) => {
            guideLease(tx, command, (_lease, generation) => {
              if (command.locatorGenerationKey !== generation.key || !command.locatorKey.startsWith(command.programmePrefix)) {
                setResult({ status: "ok", row: null }); return;
              }
              const request = tx.objectStore(STORE.guideProgrammes).get(command.locatorKey);
              request.onerror = () => fail(classify(request.error));
              request.onsuccess = () => setResult({ status: "ok", row: request.result || null });
            }, setResult, fail);
          });
        case "durableGuideWindow":
          return transaction(database, [STORE.guideLeases, STORE.guideGenerations, STORE.guideProgrammes, STORE.guideTimeline, STORE.guideMigration], "readonly", command.operationId, (tx, setResult, fail) => {
            guideLease(tx, command, (_lease, generation) => {
              const store = tx.objectStore(STORE.guideProgrammes);
              const rows = [];
              let payloadBytes = 0;
              let visits = 0;
              const finish = (truncated) => setResult({
                status: "ok",
                rows: rows,
                nextStartMs: truncated && rows.length > 0 ? rows[rows.length - 1].startMs : null,
                truncated: truncated,
                payloadBytes: payloadBytes,
              });
              const accept = (row, locatorKey, advance) => {
                if (row.effectiveEndMs <= command.fromMs) { advance(); return; }
                const bytes = guideProgrammeBytes(row);
                if (rows.length >= command.limit || payloadBytes + bytes > command.payloadByteLimit) {
                  finish(true); return;
                }
                rows.push(Object.assign({ locatorKey: locatorKey }, row));
                payloadBytes += bytes;
                advance();
              };
              const runLegacy = () => {
                const index = store.index("generationChannelStart");
                const lower = command.afterStartMs === null ? Number.MIN_SAFE_INTEGER : command.afterStartMs;
                const range = IDBKeyRange.bound(
                  [generation.key, command.channelKey, lower],
                  [generation.key, command.channelKey, command.untilMs],
                  command.afterStartMs !== null,
                  true,
                );
                const request = index.openCursor(range);
                request.onerror = () => fail(classify(request.error));
                request.onsuccess = () => {
                  const cursor = request.result;
                  if (!cursor) { finish(false); return; }
                  if (visits >= command.maxIndexVisits) {
                    setResult({ status: "needsMigration" }); return;
                  }
                  visits += 1;
                  accept(cursor.value, cursor.primaryKey, () => cursor.continue());
                };
              };
              const runTimeline = () => {
                if (generation.minStartMs === null) { finish(false); return; }
                const timeline = tx.objectStore(STORE.guideTimeline);
                const finiteIndex = timeline.index("generationChannelFiniteStart");
                const openIndex = timeline.index("generationChannelOpenStart");
                const finiteFloor = Math.max(Number.MIN_SAFE_INTEGER, command.fromMs - generation.maxFiniteSpanMs);
                const finiteLower = command.afterStartMs === null
                  ? finiteFloor
                  : Math.max(finiteFloor, command.afterStartMs);
                const openLower = command.afterStartMs === null
                  ? generation.minStartMs
                  : Math.max(generation.minStartMs, command.afterStartMs);
                const finiteRange = IDBKeyRange.bound(
                  [generation.key, command.channelKey, finiteLower],
                  [generation.key, command.channelKey, command.untilMs],
                  command.afterStartMs !== null && command.afterStartMs >= finiteFloor,
                  true,
                );
                const openRange = IDBKeyRange.bound(
                  [generation.key, command.channelKey, openLower],
                  [generation.key, command.channelKey, command.untilMs],
                  command.afterStartMs !== null && command.afterStartMs >= generation.minStartMs,
                  true,
                );
                let finiteCursor;
                let openCursor;
                let finiteReady = false;
                let openReady = false;
                const advance = (kind, cursor) => {
                  if (kind === "finite") finiteReady = false;
                  else openReady = false;
                  cursor.continue();
                };
                const pump = () => {
                  if (!finiteReady || !openReady) return;
                  if (!finiteCursor && !openCursor) { finish(false); return; }
                  let kind;
                  let cursor;
                  if (!openCursor || (finiteCursor && (
                    finiteCursor.value.startMs < openCursor.value.startMs ||
                    (finiteCursor.value.startMs === openCursor.value.startMs && finiteCursor.primaryKey < openCursor.primaryKey)
                  ))) {
                    kind = "finite"; cursor = finiteCursor;
                  } else {
                    kind = "open"; cursor = openCursor;
                  }
                  if (visits >= command.maxIndexVisits) { setResult({ status: "limit" }); return; }
                  visits += 1;
                  const fullRequest = store.get(cursor.primaryKey);
                  fullRequest.onerror = () => fail(classify(fullRequest.error));
                  fullRequest.onsuccess = () => {
                    const row = fullRequest.result;
                    if (!row) { advance(kind, cursor); return; }
                    accept(row, cursor.primaryKey, () => advance(kind, cursor));
                  };
                };
                const finiteRequest = finiteIndex.openCursor(finiteRange);
                finiteRequest.onerror = () => fail(classify(finiteRequest.error));
                finiteRequest.onsuccess = () => { finiteCursor = finiteRequest.result; finiteReady = true; pump(); };
                const openRequest = openIndex.openCursor(openRange);
                openRequest.onerror = () => fail(classify(openRequest.error));
                openRequest.onsuccess = () => { openCursor = openRequest.result; openReady = true; pump(); };
              };
              const migrationRequest = tx.objectStore(STORE.guideMigration).get("legacy-v3");
              migrationRequest.onerror = () => fail(classify(migrationRequest.error));
              migrationRequest.onsuccess = () => {
                const migration = migrationRequest.result;
                if (generation.timelineMigrated === true || (migration && migration.complete)) runTimeline();
                else runLegacy();
              };
            }, setResult, fail);
          });
        case "guideNowNext":
          return transaction(database, [STORE.guideLeases, STORE.guideGenerations, STORE.guideProgrammes], "readonly", command.operationId, (tx, setResult, fail) => {
            guideLease(tx, command, (_lease, generation) => {
              const store = tx.objectStore(STORE.guideProgrammes);
              const prefix = command.channelProgrammePrefix;
              let current = undefined;
              let next = undefined;
              const complete = () => {
                if (current !== undefined && next !== undefined) setResult({ status: "ok", current: current, next: next });
              };
              const currentRequest = store.openCursor(
                IDBKeyRange.bound(prefix, prefix + command.atKey + "|\uffff"),
                "prev",
              );
              currentRequest.onerror = () => fail(classify(currentRequest.error));
              currentRequest.onsuccess = () => {
                const cursor = currentRequest.result;
                if (!cursor) { current = null; complete(); return; }
                const row = cursor.value;
                if (row.effectiveEndMs > command.atMs) { current = row; complete(); } else cursor.continue();
              };
              const nextRequest = store.openCursor(
                IDBKeyRange.bound(prefix + command.atKey + "|\uffff", prefix + "\uffff", true, false),
                "next",
              );
              nextRequest.onerror = () => fail(classify(nextRequest.error));
              nextRequest.onsuccess = () => {
                const cursor = nextRequest.result;
                next = cursor ? cursor.value : null;
                complete();
              };
            }, setResult, fail);
          });
        case "guideBeginPrune":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const feedRequest = states.get(command.feedStateKey);
            feedRequest.onerror = () => fail(classify(feedRequest.error));
            feedRequest.onsuccess = () => {
              const feed = feedRequest.result;
              if (
                !feed || feed.deleted || feed.activeGeneration === null ||
                feed.revision !== command.expectedRevision || feed.mutation !== command.expectedMutationEpoch
              ) { setResult({ status: "superseded", current: guideSnapshot(feed) }); return; }
              if (
                command.retention.anchorMs !== feed.retention.anchorMs ||
                command.retention.retainedFromMs < feed.retention.retainedFromMs ||
                command.retention.retainedUntilMs > feed.retention.retainedUntilMs
              ) { setResult({ status: "limit" }); return; }
              const sourceRequest = states.get(command.sourceStateKey);
              sourceRequest.onerror = () => fail(classify(sourceRequest.error));
              sourceRequest.onsuccess = () => {
                const source = sourceRequest.result;
                if (!source || source.deleted || source.epoch !== feed.sourceEpoch) {
                  setResult({ status: "superseded", current: null }); return;
                }
                const generations = tx.objectStore(STORE.guideGenerations);
                const allocate = () => {
                  const generation = feed.nextGeneration;
                  if (!Number.isSafeInteger(generation) || generation <= 0) {
                    fail("AIR_IDB_GENERATION_EXHAUSTED"); return;
                  }
                  const component = sortableKey(generation);
                  const generationKey = command.generationPrefix + component;
                  feed.nextGeneration = generation + 1;
                  feed.latestGeneration = generation;
                  feed.mutation += 1;
                  source.mutation += 1;
                  const row = {
                    key: generationKey,
                    sourceKey: command.sourceKey,
                    sourceStateKey: command.sourceStateKey,
                    feedId: command.feedId,
                    sourceEpoch: source.epoch,
                    sourceEpochKey: command.sourceKey + "|" + sortableKey(source.epoch),
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
                  states.put(feed); states.put(source); generations.put(row);
                  guideQueuePut(tx, row.key, row.expiresAt);
                  setResult({ status: "ok", generation: generation, mutationEpoch: feed.mutation, sourceEpoch: source.epoch });
                };
                if (feed.latestGeneration === null) { allocate(); return; }
                const oldKey = command.generationPrefix + sortableKey(feed.latestGeneration);
                const oldRequest = generations.get(oldKey);
                oldRequest.onerror = () => fail(classify(oldRequest.error));
                oldRequest.onsuccess = () => {
                  const old = oldRequest.result;
                  if (old) { old.status = "superseded"; old.expiresAt = 0; generations.put(old); guideQueuePut(tx, old.key, 0); }
                  allocate();
                };
              };
            };
          });
        case "guideFinishPruneUnchanged":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const generations = tx.objectStore(STORE.guideGenerations);
            const request = generations.get(command.generationKey);
            request.onerror = () => fail(classify(request.error));
            request.onsuccess = () => {
              const generation = request.result;
              if (!generation || generation.status !== "staging") { setResult({ status: "stale" }); return; }
              const states = tx.objectStore(STORE.guideStates);
              const feedRequest = states.get(generation.feedStateKey);
              feedRequest.onerror = () => fail(classify(feedRequest.error));
              feedRequest.onsuccess = () => {
                const feed = feedRequest.result;
                if (!feed || feed.latestGeneration !== generation.generation) {
                  setResult({ status: "superseded", current: guideSnapshot(feed) }); return;
                }
                const sourceRequest = states.get(generation.sourceStateKey);
                sourceRequest.onerror = () => fail(classify(sourceRequest.error));
                sourceRequest.onsuccess = () => {
                  const source = sourceRequest.result;
                  if (!source || source.deleted || source.epoch !== generation.sourceEpoch) {
                    setResult({ status: "superseded", current: null }); return;
                  }
                  generation.status = "abandoned"; generation.expiresAt = 0;
                  feed.latestGeneration = null;
                  generations.put(generation); states.put(feed); guideQueuePut(tx, generation.key, 0);
                  setResult({ status: "unchanged", current: guideSnapshot(feed) });
                };
              };
            };
          });
        case "guideDelete":
          return transaction(database, [STORE.guideStates, STORE.guideGenerations, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const sourceRequest = states.get(command.sourceStateKey);
            sourceRequest.onerror = () => fail(classify(sourceRequest.error));
            sourceRequest.onsuccess = () => {
              const source = sourceRequest.result || guideSourceState(command.sourceStateKey, command.sourceKey);
              const feedRequest = states.get(command.feedStateKey);
              feedRequest.onerror = () => fail(classify(feedRequest.error));
              feedRequest.onsuccess = () => {
                const feed = feedRequest.result || guideFeedState(command.feedStateKey, command.sourceKey, command.feedId, source.epoch);
                if (command.conditional && (
                  feed.activeGeneration === null || feed.revision !== command.expectedRevision ||
                  feed.mutation !== command.expectedMutationEpoch
                )) { setResult({ status: "superseded", current: guideSnapshot(feed) }); return; }
                const generations = tx.objectStore(STORE.guideGenerations);
                const mark = (generation, status) => {
                  if (generation === null) return;
                  const key = command.generationPrefix + sortableKey(generation);
                  const request = generations.get(key);
                  request.onsuccess = () => {
                    const row = request.result;
                    if (row) { row.status = status; row.expiresAt = 0; generations.put(row); guideQueuePut(tx, row.key, 0); }
                  };
                };
                const hadActive = feed.activeGeneration !== null;
                const hadStagedOnly = !hadActive && feed.latestGeneration !== null;
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
                if (hadActive) source.activeFeedCount = Math.max(0, source.activeFeedCount - 1);
                if (hadStagedOnly) source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                source.mutation += 1;
                states.put(feed); states.put(source);
                setResult({ status: "deleted", revision: feed.revision });
              };
            };
          });
        case "guideDeleteSource":
          return transaction(database, [STORE.guideStates, STORE.guideCleanupQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            const states = tx.objectStore(STORE.guideStates);
            const request = states.get(command.sourceStateKey);
            request.onerror = () => fail(classify(request.error));
            request.onsuccess = () => {
              const source = request.result || guideSourceState(command.sourceStateKey, command.sourceKey);
              if (command.conditional && (source.epoch !== command.sourceEpoch || source.mutation !== command.sourceMutation)) {
                setResult({ status: "superseded", activeFeedCount: source.deleted ? 0 : source.activeFeedCount, stagedOnlyFeedCount: source.deleted ? 0 : source.stagedOnlyFeedCount });
                return;
              }
              const active = source.deleted ? 0 : source.activeFeedCount;
              const staged = source.deleted ? 0 : source.stagedOnlyFeedCount;
              const oldEpoch = source.epoch;
              source.epoch += 1;
              source.mutation += 1;
              source.activeFeedCount = 0;
              source.stagedOnlyFeedCount = 0;
              source.deleted = true;
              states.put(source);
              tx.objectStore(STORE.guideCleanupQueue).put({
                key: "QS|" + command.sourceKey + "|" + sortableKey(oldEpoch),
                kind: "source",
                sourceKey: command.sourceKey,
                sourceEpoch: oldEpoch,
                cleanupAt: 0,
              });
              setResult({ status: "deleted", activeFeedCount: active, stagedOnlyFeedCount: staged });
            };
          });
        case "guideCleanup":
          return transaction(database, [
            STORE.guideStates,
            STORE.guideGenerations,
            STORE.guideChannels,
            STORE.guideProgrammes,
            STORE.guideTimeline,
            STORE.guideLeases,
            STORE.guideCleanupQueue,
          ], "readwrite", command.operationId, (tx, setResult, fail) => {
            const queue = tx.objectStore(STORE.guideCleanupQueue);
            const queueIndex = queue.index("cleanupAt");
            const finish = (removedRows) => {
              const moreRequest = queueIndex.openKeyCursor(IDBKeyRange.upperBound(command.nowMs));
              moreRequest.onerror = () => fail(classify(moreRequest.error));
              moreRequest.onsuccess = () => setResult({
                status: "ok",
                removedRows: removedRows,
                hasMore: moreRequest.result !== null,
              });
            };
            const processGeneration = (generation, queueRow) => {
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
              const leases = tx.objectStore(STORE.guideLeases);
              const leaseRequest = leases.index("generationKey").openCursor(IDBKeyRange.only(generation.key));
              leaseRequest.onerror = () => fail(classify(leaseRequest.error));
              leaseRequest.onsuccess = () => {
                const cursor = leaseRequest.result;
                if (cursor) {
                  const lease = cursor.value;
                  if (lease.expiresAt <= command.nowMs) {
                    cursor.delete();
                    queueRow.cleanupAt = 0;
                  } else {
                    queueRow.cleanupAt = lease.expiresAt;
                  }
                  queue.put(queueRow);
                  finish(0);
                  return;
                }
                generation.cleanupStarted = true;
                generation.status = generation.status === "staging" ? "expired" : generation.status;
                tx.objectStore(STORE.guideGenerations).put(generation);
                let remaining = command.maxRows;
                let removed = 0;
                const tasks = [
                  [STORE.guideChannels, generation.channelPrefix, false],
                  [STORE.guideProgrammes, generation.programmePrefix, true],
                ];
                let taskIndex = 0;
                const runTask = () => {
                  if (remaining === 0 || taskIndex >= tasks.length) { checkEmpty(); return; }
                  const task = tasks[taskIndex++];
                  const request = tx.objectStore(task[0]).openCursor(rangeForPrefix(task[1]));
                  request.onerror = () => fail(classify(request.error));
                  request.onsuccess = () => {
                    const cursor = request.result;
                    if (!cursor || remaining === 0) { runTask(); return; }
                    cursor.delete();
                    if (task[2]) tx.objectStore(STORE.guideTimeline).delete(cursor.primaryKey);
                    remaining -= 1;
                    removed += 1;
                    cursor.continue();
                  };
                };
                const checkEmpty = () => {
                  const channelsCount = tx.objectStore(STORE.guideChannels)
                    .openKeyCursor(rangeForPrefix(generation.channelPrefix));
                  const programmesCount = tx.objectStore(STORE.guideProgrammes)
                    .openKeyCursor(rangeForPrefix(generation.programmePrefix));
                  let pending = 2;
                  let hasPayload = false;
                  const done = (request) => {
                    hasPayload = hasPayload || request.result !== null;
                    pending -= 1;
                    if (pending !== 0) return;
                    if (!hasPayload) {
                      tx.objectStore(STORE.guideGenerations).delete(generation.key);
                      if (queueRow.kind === "source") {
                        queueRow.cleanupAt = 0;
                        queue.put(queueRow);
                      } else {
                        queue.delete(queueRow.key);
                      }
                      const states = tx.objectStore(STORE.guideStates);
                      const feedRequest = states.get(generation.feedStateKey);
                      feedRequest.onerror = () => fail(classify(feedRequest.error));
                      feedRequest.onsuccess = () => {
                        const feed = feedRequest.result;
                        if (feed && feed.sourceEpoch === generation.sourceEpoch && feed.latestGeneration === generation.generation) {
                          feed.latestGeneration = null;
                          feed.mutation += 1;
                          states.put(feed);
                          const sourceRequest = states.get("GS|" + generation.sourceKey);
                          sourceRequest.onerror = () => fail(classify(sourceRequest.error));
                          sourceRequest.onsuccess = () => {
                            const source = sourceRequest.result;
                            if (source && source.epoch === generation.sourceEpoch && !source.deleted) {
                              if (feed.activeGeneration === null) source.stagedOnlyFeedCount = Math.max(0, source.stagedOnlyFeedCount - 1);
                              source.mutation += 1;
                              states.put(source);
                            }
                          };
                        }
                      };
                    } else {
                      queueRow.cleanupAt = 0;
                      queue.put(queueRow);
                    }
                    finish(removed);
                  };
                  channelsCount.onerror = () => fail(classify(channelsCount.error));
                  programmesCount.onerror = () => fail(classify(programmesCount.error));
                  channelsCount.onsuccess = () => done(channelsCount);
                  programmesCount.onsuccess = () => done(programmesCount);
                };
                runTask();
              };
            };
            const queueRequest = queueIndex.openCursor(IDBKeyRange.upperBound(command.nowMs));
            queueRequest.onerror = () => fail(classify(queueRequest.error));
            queueRequest.onsuccess = () => {
              const cursor = queueRequest.result;
              if (!cursor) { setResult({ status: "ok", removedRows: 0, hasMore: false }); return; }
              const queueRow = cursor.value;
              if (queueRow.kind === "source") {
                const sourceEpochKey = queueRow.sourceKey + "|" + sortableKey(queueRow.sourceEpoch);
                const generationRequest = tx.objectStore(STORE.guideGenerations)
                  .index("sourceEpochKey").openCursor(IDBKeyRange.only(sourceEpochKey));
                generationRequest.onerror = () => fail(classify(generationRequest.error));
                generationRequest.onsuccess = () => {
                  const generationCursor = generationRequest.result;
                  if (!generationCursor) { queue.delete(queueRow.key); finish(0); return; }
                  const generation = generationCursor.value;
                  processGeneration(generation, {
                    key: queueRow.key,
                    kind: "source",
                    sourceKey: queueRow.sourceKey,
                    sourceEpoch: queueRow.sourceEpoch,
                    generationKey: generation.key,
                    cleanupAt: 0,
                  });
                };
              } else {
                const generationRequest = tx.objectStore(STORE.guideGenerations).get(queueRow.generationKey);
                generationRequest.onerror = () => fail(classify(generationRequest.error));
                generationRequest.onsuccess = () => processGeneration(generationRequest.result, queueRow);
              }
            };
          });
        case "guideDebugDump":
          return transaction(database, [
            STORE.guideStates,
            STORE.guideGenerations,
            STORE.guideChannels,
            STORE.guideProgrammes,
            STORE.guideTimeline,
            STORE.guideMigration,
            STORE.guideLeases,
            STORE.guideCleanupQueue,
          ], "readonly", command.operationId, (tx, setResult, fail) => {
            const stores = [
              STORE.guideStates,
              STORE.guideGenerations,
              STORE.guideChannels,
              STORE.guideProgrammes,
              STORE.guideTimeline,
              STORE.guideMigration,
              STORE.guideLeases,
              STORE.guideCleanupQueue,
            ];
            const records = [];
            let index = 0;
            const nextStore = () => {
              if (index >= stores.length || records.length >= command.limit) {
                setResult({ status: "ok", records: records });
                return;
              }
              const request = tx.objectStore(stores[index++]).openCursor();
              request.onerror = () => fail(classify(request.error));
              request.onsuccess = () => {
                const cursor = request.result;
                if (!cursor) { nextStore(); return; }
                records.push(stringify(cursor.value));
                if (records.length >= command.limit) setResult({ status: "ok", records: records });
                else cursor.continue();
              };
            };
            nextStore();
          });
        case "deleteSource":
          return transaction(database, [STORE.sources, STORE.generations, STORE.orphanQueue], "readwrite", command.operationId, (tx, setResult, fail) => {
            getJson(tx.objectStore(STORE.sources), command.sourceKey, (source) => {
              if (!source || source.deleted) { setResult(null); return; }
              const active = source.activeGeneration;
              source.activeGeneration = null;
              source.revision += 1;
              source.deleted = true;
              tx.objectStore(STORE.sources).put(stringify(source), command.sourceKey);
              if (active !== null) {
                const generationKey = command.generationPrefix + sortableKey(active);
                const queueKey = "Q|" + command.nowKey + "|" + command.sourceComponent + "|" + sortableKey(active);
                getJson(tx.objectStore(STORE.generations), generationKey, (row) => {
                  if (row) {
                    row.queueKey = queueKey;
                    tx.objectStore(STORE.generations).put(stringify(row), generationKey);
                    tx.objectStore(STORE.orphanQueue).put(stringify({ generationKey: generationKey }), queueKey);
                  }
                }, fail);
              }
              setResult(null);
            }, fail);
          });
        case "cleanup":
          return transaction(database, ALL_STORES, "readwrite", command.operationId, (tx, setResult, fail) => {
            const cursorRequest = tx.objectStore(STORE.orphanQueue).openCursor();
            cursorRequest.onerror = () => fail(classify(cursorRequest.error));
            cursorRequest.onsuccess = () => {
              const cursor = cursorRequest.result;
              if (!cursor) { setResult({ removedRows: 0, hasMore: false }); return; }
              let row;
              try { row = parse(cursor.value); } catch (_) { fail("AIR_IDB_CORRUPT"); return; }
              command.queueKey = cursor.key;
              getJson(tx.objectStore(STORE.generations), row.generationKey, (generationRow) => {
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
                deletePrefixRows(tx, command, generationRow, setResult, fail);
              }, fail);
            };
          });
          default:
            throw error("AIR_IDB_COMMAND");
        }
      });
    };
    root[runtimeKey] = { execute: execute };
  }
  return root[runtimeKey].execute(databaseName, commandJson);
}
