# Browser catalog runtime

`AirIndexedDbRuntime.js` is the maintained IndexedDB transaction runtime used by
both Kotlin/JS and Kotlin/WasmJS. `es5/AirIndexedDbRuntime.js` is its checked-in
ES5 syntax transform for Kotlin/JS's inline JavaScript parser. It is not a
second implementation.

After changing the maintained runtime, regenerate the ES5 file with TypeScript
and run both real-browser suites:

```bash
tsc --allowJs --checkJs false --target ES5 --module none \
  --outDir src/browserIndexedDb/es5 \
  src/browserIndexedDb/AirIndexedDbRuntime.js \
  --skipLibCheck --noEmitOnError false --lib ES2015,DOM

CHROME_BIN=/usr/bin/chromium ./gradlew \
  jsBrowserTest wasmJsBrowserTest --max-workers=2
```

The runtime stores rebuildable, redacted protocol metadata only. Credentials,
playback URLs and headers, artwork bytes, and whole-catalog observable state do
not belong in this database.
