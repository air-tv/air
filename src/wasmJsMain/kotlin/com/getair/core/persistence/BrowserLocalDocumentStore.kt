package com.getair.core.persistence

import kotlinx.browser.localStorage

/** Non-secret browser persistence. Credentials must never be written here. */
fun browserLocalDocumentStore(namespace: String = "air"): LocalDocumentStore = BrowserDocumentStore(
    namespace = namespace,
    readValue = localStorage::getItem,
    writeValue = localStorage::setItem,
    removeValue = localStorage::removeItem,
)
