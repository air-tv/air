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
    });
    const ALL_STORES = Object.freeze(Object.values(STORE));
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
        try {
          openRequest = root.indexedDB.open(name, 2);
        } catch (failure) {
          reject(error(classify(failure)));
          return;
        }
        openRequest.onupgradeneeded = (event) => {
          const database = openRequest.result;
          if (event.oldVersion < 2) {
            for (const store of Array.from(database.objectStoreNames)) database.deleteObjectStore(store);
            for (const store of ALL_STORES) {
              if (store !== STORE.catalogRecords && store !== STORE.channelRecords) database.createObjectStore(store);
            }
            database.createObjectStore(STORE.catalogRecords, { keyPath: "recordKey" })
              .createIndex("orderKey", "orderKey", { unique: true });
            database.createObjectStore(STORE.channelRecords, { keyPath: "recordKey" })
              .createIndex("orderKey", "orderKey", { unique: true });
          }
        };
        openRequest.onblocked = () => reject(error("AIR_IDB_BLOCKED"));
        openRequest.onerror = () => reject(error(classify(openRequest.error)));
        openRequest.onsuccess = () => {
          const database = openRequest.result;
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
