package com.getair.core.household

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class HouseholdTest {
    @Test
    fun managesProfilesAndSettingsLocally() = runTest {
        val repository = LocalFirstHouseholdRepository(InMemoryHouseholdStore())
        val livingRoom = profile("living", "Living Room", "LR")
        val kids = profile("kids", "Kids", "K", isKids = true)

        repository.upsertProfile(livingRoom)
        repository.upsertProfile(kids)
        assertEquals(livingRoom.id, repository.state.value.selectedProfileId)

        repository.selectProfile(kids.id)
        repository.updateProfilePreferences(kids.id) {
            it.copy(autoplayNextEpisode = false, subtitlesEnabled = false)
        }
        repository.updateDeviceSettings { it.copy(oledBlack = true, diagnosticsOverlay = true) }

        val updated = repository.state.value
        assertEquals(kids.id, updated.selectedProfileId)
        assertFalse(updated.profilePreferences.getValue(kids.id).autoplayNextEpisode)
        assertTrue(updated.deviceSettings.oledBlack)
        assertTrue(updated.deviceSettings.diagnosticsOverlay)

        repository.removeProfile(kids.id)
        assertEquals(livingRoom.id, repository.state.value.selectedProfileId)
        repository.removeProfile(livingRoom.id)
        assertNull(repository.state.value.selectedProfileId)
    }

    @Test
    fun selectingTheCurrentProfileIsAStableNoOp() = runTest {
        val repository = LocalFirstHouseholdRepository(InMemoryHouseholdStore())
        val livingRoom = profile("living", "Living Room", "LR")
        repository.upsertProfile(livingRoom)
        val revision = repository.state.value.revision

        repository.selectProfile(livingRoom.id)
        repository.upsertProfile(livingRoom)
        repository.removeProfile(HouseholdProfileId("missing"))
        repository.updateDeviceSettings { it }

        assertEquals(revision, repository.state.value.revision)
    }

    @Test
    fun remainsLocalOnlyUntilASyncSourceIsInjected() = runTest {
        val store = InMemoryHouseholdStore()
        val local = LocalFirstHouseholdRepository(store)
        assertIs<HouseholdRefreshResult.LocalOnly>(local.refresh())

        val synced = LocalFirstHouseholdRepository(store) { previous ->
            previous.copy(deviceSettings = previous.deviceSettings.copy(oledBlack = true), revision = 9)
        }
        val result = assertIs<HouseholdRefreshResult.Updated>(synced.refresh())
        assertEquals(9, result.revision)
        assertTrue(synced.state.value.deviceSettings.oledBlack)
    }
}

private fun profile(id: String, name: String, initials: String, isKids: Boolean = false) = HouseholdProfile(
    id = HouseholdProfileId(id),
    name = name,
    initials = initials,
    isKids = isKids,
)
