import fetch from 'node-fetch'
import throttledQueue from 'throttled-queue'
import dotenv from 'dotenv'

dotenv.config()

// Set equality (for basic types)
// cf. https://stackoverflow.com/questions/31128855/comparing-ecma6-sets-for-equality

function eqSet(as, bs) {
    if (as.size !== bs.size) return false;
    for (var a of as) if (!bs.has(a)) return false;
    return true;
}

// check equality between 2 resources Sets
function eqResources(as, bs) {
    if (as.size !== bs.size) return false;
    for (let a of as) {
        let found = false
        for (let b of bs) {
            if (a.url == b.url) {
                found = true
                if (!(a.title == b.title && a.description == b.description)) {
                    return false;
                }
            }
        }
        if (!found) {
            return false
        }
    }
    return true;
}

// throttle API requests to avoid overloading the servers
const throttle = throttledQueue(parseInt(process.env.callRateNrCalls), parseInt(process.env.callRateDuration))
function fetchThrottle(...params) {
    return throttle(() => {return fetch(...params)})
} 

export { eqSet, eqResources, fetchThrottle }